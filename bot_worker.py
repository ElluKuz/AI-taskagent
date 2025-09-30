# -*- coding: utf-8 -*-
import asyncio, os, tempfile, csv, io
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import List, Tuple
from uuid import uuid4
import re
from datetime import date
from app_config import BOT_TOKEN, TZ, WORK_END_HOUR, VADIM_CHAT_ID, ASSISTANT_CHAT_IDS

from telegram import (Update, InlineKeyboardButton as B, InlineKeyboardMarkup as KM, Message, InputFile)
from telegram.error import Forbidden, BadRequest, TelegramError
from telegram.error import BadRequest as TG_BadRequest
from telegram.constants import ParseMode, ChatType
from telegram.ext import Application, ContextTypes, ConversationHandler, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from html import escape as h


from db import (
    list_unique_assignees, get_tasks_by_tid_openlike, insert_task, update_task_assignment, get_task,
    set_task_status, set_task_deadline, set_task_text, set_task_priority, mark_cancelled,
    track_chat, set_last_chat_offset, get_last_chat_offset, list_tracked_chats,
    fetch_proposed_tasks, find_open_tasks_for_user, get_priority, get_tasks_by_assignee_openlike,
    get_all_tasks, get_reassignments_for_task, get_deadline_changes_for_task, enqueue_outbox,
    assignee_exists_by_tid, get_nickname_by_tid, get_overdue_open_tasks
)


from llm import llm_route
from nlp import looks_like_task, extract_deadline, extract_priority, strip_bot_mention, detect_assignee
from voice import transcribe_telegram_file


import logging, sys
LOG_PATH = "/home/loyo/projects/VadimsTasks/bot.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)
logging.getLogger("telegram.ext").setLevel(logging.INFO)
logger = logging.getLogger("bot")

ASK_DEADLINE, REASON_NOT_RELEVANT = range(2)


TZINFO = ZoneInfo(TZ)
from datetime import timezone  # наверху файла уже есть datetime/timedelta

WEEKDAYS = {0,1,2,3,4}        # пн–пт
GRACE_MINUTES = 30            # до 18:30 можно слать алёрты о новых задачах

MONTHS_RU = ["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"]
ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")



async def safe_edit_text(msg, text, reply_markup=None):
    try:
        await msg.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except TG_BadRequest as e:
        if "not modified" in str(e).lower():
            return
        raise
def _in_task_alert_window(dt_local):
    local = dt_local.astimezone(TZINFO)
    if local.weekday() not in WEEKDAYS:
        return False
    if local.hour < 9:
        return False
    # разрешаем до 18:30 включительно
    if local.hour > 18 or (local.hour == 18 and local.minute > GRACE_MINUTES):
        return False
    return True

def _next_work_morning(dt_local):
    d = dt_local.astimezone(TZINFO)
    # если после 18:30 — на следующий день; если до 09:00 — на сегодня 09:00
    if d.hour > 18 or (d.hour == 18 and d.minute > GRACE_MINUTES):
        d = d + timedelta(days=1)
    d = d.replace(hour=9, minute=0, second=0, microsecond=0)
    while d.weekday() not in WEEKDAYS:
        d = (d + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ===== Буферы на день =========================================================
# MESSAGE_BUFFER: (mid, username, full_name, text, dt)
MESSAGE_BUFFER: dict[str, list[tuple[int, str, str, str, datetime]]] = {}
# VOICE_BUFFER:   (mid, username, full_name, file_id, dt)
VOICE_BUFFER: dict[str, list[tuple[int, str, str, str, datetime]]] = {}

# Состояния мастера ревью/правок
RV_WAIT_DESC, RV_WAIT_ASSIGNEE_PICK, RV_WAIT_DEADLINE, RV_WAIT_PRIORITY, RV_WAIT_CANCEL_REASON, RV_WAIT_PROOF = range(6)

# Состояние показа списков
CHECK_STATE = {}     # assistant_id -> {"rows": [...], "idx": 0, "msg_id": int}
FLOW_STATE = {}      # user_id -> {"rows": [...], "idx": 0, "msg_id": int, "assignee": (name, tid)}

BOT_USERNAME = ""
ALLOWED_FLOW_VIEWERS = { str(VADIM_CHAT_ID), *map(str, ASSISTANT_CHAT_IDS) }

MYTASK_STATE = {}
NT_TEXT, NT_ASSIGNEE, NT_PRIORITY, NT_DEADLINE = range(4)

# --------------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ШТУКИ
# --------------------------------------------------------------------------------
async def first_touch_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Если пользователь пишет команду в ЛС и его нет в базе assignees — один раз показываем его Telegram ID.
    """
    # работаем только в приватном чате
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    user = update.effective_user
    if not user:
        return
    uid = str(user.id)

    # если уже есть в базе — выходим
    if assignee_exists_by_tid(uid):
        return

    # чтобы не спамить в рамках текущей сессии бота
    if context.user_data.get("sent_tid_once"):
        return

    msg = (
        "Привет! Похоже, это первый контакт.\n\n"
        f"Твой Telegram ID: <code>{uid}</code>\n\n"
        "Перешли этот ID администратору (чтобы тебя добавили в базу исполнителей)."
    )
    try:
        # ответим на ту же команду
        if update.effective_message:
            await update.effective_message.reply_text(msg, parse_mode=ParseMode.HTML)
        else:
            await context.bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.HTML)
    finally:
        context.user_data["sent_tid_once"] = True

async def notify_assistants(context, text: str):
    now = datetime.now(TZINFO)
    if _in_task_alert_window(now):
        # можно отправить немедленно
        for cid in ASSISTANT_CHAT_IDS:
            try:
                await context.bot.send_message(
                    chat_id=str(cid), text=text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except Exception as e:
                logger.warning("Не удалось отправить ассистенту %s: %s", cid, e)
    else:
        # вне окна — положим в outbox до 09:00 ближайшего рабочего дня
        not_before = _next_work_morning(now)
        for cid in ASSISTANT_CHAT_IDS:
            try:
                enqueue_outbox(str(cid), text, None, not_before)
            except Exception as e:
                logger.warning("enqueue_outbox failed for %s: %s", cid, e)

async def copy_to_assistants(context, from_chat_id, message_id):
    for cid in ASSISTANT_CHAT_IDS:
        try:
            await context.bot.copy_message(chat_id=str(cid), from_chat_id=from_chat_id, message_id=message_id)
        except Exception as e:
            logger.warning("Не удалось скопировать ассистенту %s: %s", cid, e)

def get_assignee_name_list() -> list[str]:
    rows = list_unique_assignees()
    return [r[0] for r in rows if r and r[0]]

def km_review_nav(idx: int, total: int, task_id: int, context: str):
    left_dis  = idx <= 0
    right_dis = idx >= total - 1
    row_nav = [
        B("◀️", callback_data=f"nav:{context}:{max(0, idx-1)}") if not left_dis else B("◀️", callback_data="nav:noop"),
        B(f"{idx+1}/{total}", callback_data="nav:noop"),
        B("▶️", callback_data=f"nav:{context}:{min(total-1, idx+1)}") if not right_dis else B("▶️", callback_data="nav:noop"),
    ]
    row_act1 = [B("▶️ Поставить",      callback_data=f"act:approve:{task_id}")]
    row_act2 = [B("✏️ Изменить",       callback_data=f"act:edit:{task_id}"),
                B("🔁 Переназначить",  callback_data=f"act:reassign:{task_id}")]
    row_act3 = [B("🗑 Неактуально",     callback_data=f"act:cancel:{task_id}")]
    return KM([row_nav, row_act1, row_act2, row_act3])



def km_pick_assignee(prefix: str):
    rows = list_unique_assignees()
    rows = [(n, tid) for n, tid in rows if str(tid or "").strip()]  # только у кого есть telegram_id
    if not rows:
        return KM([[B("Нет исполнителей с Telegram ID", callback_data="nav:noop")]])
    return KM([[B(n, callback_data=f"{prefix}:{tid}")] for n, tid in rows])

def fmt_date_human(yyyy_mm_dd: str | None) -> str:
    s = (yyyy_mm_dd or "").strip()
    if not s:
        return "—"
    try:
        y,m,d = s.split("-")
        y, m, d = int(y), int(m), int(d)
        return f"{d} {MONTHS_RU[m-1]} {y}"
    except Exception:
        return s

def fmt_assignee_with_nick(name: str, tid: str | None) -> str:
    name_disp = name or "—"
    nick = (get_nickname_by_tid(tid or "") or "").strip()
    if nick:
        if not nick.startswith("@"):
            nick = "@" + nick
        return f"{name_disp} ({nick})"
    return name_disp

def initial_text(
    task_text: str,
    assignee: str,
    deadline: str,
    task_id: int,
    priority: str,
    link: str | None = None
) -> str:
    # tid достаём из самой задачи, чтобы корректно взять ник
    tid = None
    try:
        t = get_task(task_id)
        tid = t["telegram_id"] if t else None
    except Exception:
        tid = None

    link_part = f'\n🔗 <a href="{link}">Оригинал</a>' if (link or "").strip() else ""
    return (
        "👋 Привет! Для тебя зафиксирована новая задача.\n\n"
        f"{priority_block(priority)}\n"
        f"🧩 <b>{h(task_text)}</b>\n"
        f"👤 Исполнитель: {fmt_assignee_with_nick(assignee, tid)}\n"
        f"📅 Дедлайн: {fmt_date_human(deadline)}\n"
        f"ID: #{task_id}"
        f"{link_part}\n\n"
        "Я аккуратно буду напоминать, так что лучше не игнорировать 😉\n"
        "Когда будет готово, воспользуйся командой /mytasks"
    )
   


def priority_human(priority: str) -> str:
    return "важная" if (priority or "normal") == "high" else "обычная"

def priority_block(priority: str) -> str:
    if (priority or "normal") == "high":
        return "🔥 Приоритет: Важная — дедлайн перенести нельзя."
    return "⭐️ Приоритет: Обычная — можно будет сместить дедлайн при необходимости"

async def notify_new_assignment(context: ContextTypes.DEFAULT_TYPE, *, to_tid: str, task, who: str):
    text = (
        f"👋 Привет! {who} переназначил на тебя задачу.\n\n"
        f"🧩 Описание: {h(task['task'])}\n"
        f"{priority_block(task['priority'])}\n"
        f"📅 Дедлайн: {fmt_date_human(task['deadline'])}\n\n"
        f"ID: #{task['id']}\n\n"
        "Я аккуратно буду напоминать, так что лучше не игнорировать 😉\n"
        "Когда будет готово, воспользуйся командой /mytasks"
    )
    await context.bot.send_message(chat_id=str(to_tid), text=text, parse_mode=ParseMode.HTML)

async def send_initial(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id,
    task_text,
    assignee,
    deadline,
    task_id,
    priority,
) -> bool:
    kb = KM([[B("✅ Взята в работу", callback_data=f"take:{task_id}"),
              B("🙅 Это не моя задача", callback_data=f"reassign:{task_id}")]])
    try:
        link = None
        try:
            t = get_task(task_id)
            link = (t["link"] or None) if t else None
        except Exception:
            link = None
        await context.bot.send_message(
            chat_id=str(chat_id),
            text=initial_text(task_text, assignee, deadline, task_id, priority, link),
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )
        return True
    except (Forbidden, BadRequest, TelegramError) as e:
        logger.warning("DM to %s failed (initial): %s", chat_id, e)
        return False

def task_card_text(t) -> str:
    pr = "важная" if (t["priority"] or "normal") == "high" else "обычная"
    overdue = " ⛔️ ПРОСРОЧЕНО" if is_overdue_task(t) else ""
    tail = f"\n📅 Дедлайн: {fmt_date_human(t['deadline'])}{overdue}"

    # sqlite3.Row не умеет .get(), используем keys() и индексатор
    link = ""
    try:
        if "link" in t.keys():
            link = (t["link"] or "").strip()
    except Exception:
        link = ""
    link_part = f"\n🔗 <a href=\"{link}\">Оригинал</a>" if link else ""
    assignee_disp = fmt_assignee_with_nick((t['assignee'] or '—'), t['telegram_id'])
    return f"🧩 {h(t['task'])}\n👤 Исполнитель: {h(assignee_disp)}{tail}\n⭐️ Важность: {pr}{link_part}"




def ensure_private(update: Update) -> bool:
    return update.effective_chat.type == ChatType.PRIVATE

def iso_to_human_in_text(text: str) -> str:
    """Заменяет 2025-09-11 -> 11 Сентября 2025 (только в свободном тексте)."""
    def _rep(m):
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        month = MONTHS_RU[mth-1]
        month_cap = month[:1].upper() + month[1:]   # Заглавная первая буква
        return f"{d} {month_cap} {y}"
    return ISO_DATE_RE.sub(_rep, text or "")

# ---------- Парсер дат (расширенный) ----------
def _ensure_future_or_today(yyyy_mm_dd: str | None) -> str | None:
    if not yyyy_mm_dd:
        return None
    try:
        d = datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").date()
        today = datetime.now(TZINFO).date()
        if d < today:
            return None
        return yyyy_mm_dd
    except Exception:
        return None


def parse_any_date(s: str) -> str | None:
    s = (s or "").strip().lower()
    now = datetime.now(TZINFO)

    # быстрые ключевые слова
    if s in ("сегодня","today"):     return now.strftime("%Y-%m-%d")
    if s in ("завтра","tomorrow"):   return (now + timedelta(days=1)).strftime("%Y-%m-%d")
    if s in ("послезавтра","day after tomorrow","after tomorrow"):
        return (now + timedelta(days=2)).strftime("%Y-%m-%d")

    # быстрые относительные
    if s in ("через неделю","+1 неделя","+1 нед","спустя неделю"):
        return (now + timedelta(days=7)).strftime("%Y-%m-%d")
    if s in ("через 2 недели","+2 недели","+2 нед","спустя 2 недели"):
        return (now + timedelta(days=14)).strftime("%Y-%m-%d")

    m = re.fullmatch(r"(через|за)\s+(\d+)\s*(дн(я|ей)?)", s)
    if m:
        n = int(m.group(2))
        return (now + timedelta(days=n)).strftime("%Y-%m-%d")

    # «к пятнице», «к четвергу» и пр.
    if s.startswith("к "):
        wd = s.replace("к ", "").strip()
        for key, idx in _WEEKDAYS_MAP.items():
            if wd.startswith(key):
                d = _next_weekday(now, idx)
                if d.date() < now.date():
                    d = d + timedelta(days=7)
                return d.strftime("%Y-%m-%d")

    if "на следующей неделе" in s:
        next_mon = _next_weekday(now + timedelta(days=1), 0)
        next_fri = _next_weekday(next_mon, 4)
        return next_fri.strftime("%Y-%m-%d")

    if "до конца недели" in s:
        eow = _end_of_week(now)
        return eow.strftime("%Y-%m-%d")

    # 1) Строгое ISO YYYY-MM-DD — разбираем сами, без dateutil
    m = ISO_DATE_RE.fullmatch(s)
    if m:
        y, mth, d = map(int, m.groups())
        from datetime import date as _date
        try:
            _ = _date(y, mth, d)  # валидация
            return f"{y:04d}-{mth:02d}-{d:02d}"
        except Exception:
            return None

    # 2) Всё остальное — через dateutil (dayfirst=True)
    try:
        from dateutil import parser as dateparser
        d = dateparser.parse(s, dayfirst=True, fuzzy=True, default=now)
        return d.strftime("%Y-%m-%d") if d else None
    except Exception:
        return None


async def approve_and_start(task_id: int, q, context):
    t = get_task(task_id)
    if not t["assignee"] or not t["telegram_id"] or not t["deadline"]:
        await q.message.reply_text("Исполнитель/дедлайн пустые. Сначала поправь через «Изменить».")
        return

    ok = await send_initial(
        context,
        t["telegram_id"],
        t["task"],
        t["assignee"],
        t["deadline"],
        task_id,
        t["priority"]
    )

    if not ok:
        await q.message.reply_text(
            "Не могу написать исполнителю в личку: он ещё не нажал Start у бота.\n"
            "Попроси его написать боту «/start», затем снова нажми «▶️ Поставить в работу»."
        )
        return

    # если DM ушёл успешно
    set_task_status(task_id, "open")
    await q.message.reply_text("Поставил в работу. Разослал оповещения. 🧠")
    # 🔄 обновляем карусель
    await send_checktasks_carousel_refresh(q, context)
    
def km_mytask_nav(idx: int, total: int, task_id: int):
    left_dis  = idx <= 0
    right_dis = idx >= total - 1
    row_nav = [
        B("◀️", callback_data=f"mt_nav:{max(0, idx-1)}") if not left_dis else B("◀️", callback_data="mt_nav:noop"),
        B(f"{idx+1}/{total}", callback_data="mt_nav:noop"),
        B("▶️", callback_data=f"mt_nav:{min(total-1, idx+1)}") if not right_dis else B("▶️", callback_data="mt_nav:noop"),
    ]
    row_act = [
        B("✅ Готово", callback_data=f"mt_done:{task_id}"),
        B("⏰ Не успеваю", callback_data=f"mt_cant:{task_id}")
    ]
    return KM([row_nav, row_act])

# ---------- НОВОЕ: красивая карточка «Задача закрыта» ----------
def closed_card_text(t, performer_name: str | None = None) -> str:
    pr = "Важная 🔥" if (t["priority"] or "normal") == "high" else "Обычная"
    link = ""
    try:
        if "link" in t.keys():
            link = (t["link"] or "").strip()
    except Exception:
        link = ""
    link_part = f"\n🔗 <a href=\"{link}\">Оригинал</a>" if link else ""
    assignee_disp = performer_name or fmt_assignee_with_nick((t['assignee'] or '—'), t['telegram_id'])
    return (
        
        "✅ <b>ЗАДАЧА ЗАКРЫТА</b>\n"
        f"\n🧩 Описание: {h(t['task'])}\n"
        f"👤 Исполнитель: {assignee_disp}\n"
        f"📅 Дедлайн: {fmt_date_human(t['deadline'])}\n"
        f"⭐️ Важность: {pr}\n"
        f"#ID: {t['id']}\n"
        f"{link_part}\n\n"
        "Файл-подтверждение появится ниже, если его приложил пользователь\n"
    )

async def broadcast_task_closed(context: ContextTypes.DEFAULT_TYPE, t, performer_name: str | None, with_file_first_msg_id: int | None = None, src_chat: int | None = None):
    # Шлём карточку всем: шефу + помощникам
    text = closed_card_text(t, performer_name)
    # 1) текст
    try:
        await context.bot.send_message(chat_id=str(VADIM_CHAT_ID), text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.warning("Не удалось отправить VADIM_CHAT_ID закрытие: %s", e)
    await notify_assistants(context, text)
    # 2) если есть оригинальный файл (src_chat/msg) — скопируем всем после карточки
    if src_chat and with_file_first_msg_id:
        try:
            await context.bot.copy_message(chat_id=str(VADIM_CHAT_ID), from_chat_id=src_chat, message_id=with_file_first_msg_id)
        except Exception:
            pass
        await copy_to_assistants(context, from_chat_id=src_chat, message_id=with_file_first_msg_id)

def _tg_message_link(chat_id: str | int, message_id: int) -> str | None:
    """
    Супергруппа/канал без username: https://t.me/c/<abs_id_without_-100>/<msg_id>
    Пример: chat_id=-1002200866911 -> c/2200866911/12745
    """
    try:
        cid = str(chat_id)
        if cid.startswith("-100"):
            internal = cid[4:]
            return f"https://t.me/c/{internal}/{int(message_id)}"
        # если когда-нибудь будет username у чата — можно расширить
        return None
    except Exception:
        return None

def km_deadline_quick(context_tag: str, task_id: int | None = None):
    """
    context_tag: newtask | rv | postpone | take
    task_id: обязателен для rv/postpone/take; для newtask можно None
    """
    tid = str(task_id or 0)
    return KM([
        [B("Сегодня",        callback_data=f"dlq:{context_tag}:{tid}:today"),
         B("Завтра",         callback_data=f"dlq:{context_tag}:{tid}:tomorrow")],
        [B("Через неделю",   callback_data=f"dlq:{context_tag}:{tid}:plus7"),
         B("Через 2 недели", callback_data=f"dlq:{context_tag}:{tid}:plus14")],
    ])

def _date_add_days(base: datetime, days: int) -> str:
    return (base + timedelta(days=days)).strftime("%Y-%m-%d")

_WEEKDAYS_MAP = {
    "понедельник":0, "понедельникa":0, "к понедельнику":0,
    "вторник":1, "ко вторнику":1,
    "среда":2, "среду":2, "к среде":2,
    "четверг":3, "к четвергу":3,
    "пятница":4, "пятницу":4, "к пятнице":4,
    "суббота":5, "субботу":5, "к субботе":5,
    "воскресенье":6, "воскресеньеu":6, "к воскресенью":6,
}

def _next_weekday(from_dt: datetime, target_wd: int) -> datetime:
    cur = from_dt
    delta = (target_wd - cur.weekday()) % 7
    return cur + timedelta(days=delta if delta != 0 else 0)

def _end_of_week(from_dt: datetime) -> datetime:
    # Воскресенье текущей недели (или сегодня, если уже воскресенье)
    delta = 6 - from_dt.weekday()
    return from_dt + timedelta(days=max(0, delta))

async def send_checktasks_carousel_refresh(q, context):
    uid = str(q.from_user.id)
    rows = fetch_proposed_tasks(100)
    if not rows:
        await context.bot.send_message(chat_id=q.message.chat.id, text="Ничего не ждёт апрува. Пусто как в холодильнике.")
        return
    r = rows[0]
    m = await context.bot.send_message(
        chat_id=q.message.chat.id,
        text=task_card_text(r),
        reply_markup=km_review_nav(0, len(rows), r["id"], "check"),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True

    )
    CHECK_STATE[uid] = {"rows": rows, "idx": 0, "msg_id": m.message_id}

async def send_flow_carousel_refresh(q, context):
    uid = str(q.from_user.id)
    st = FLOW_STATE.get(uid) or {}
    ass = st.get("assignee")
    if not ass:
        await context.bot.send_message(chat_id=q.message.chat.id, text="Поток пуст.")
        return
    name, tid = ass
    rows = get_tasks_by_tid_openlike(tid)
    if not rows:
        await context.bot.send_message(chat_id=q.message.chat.id, text=f"{name or 'Исполнитель'}: нет открытых задач.")
        return
    r = rows[0]
    m = await context.bot.send_message(
        chat_id=q.message.chat.id,
        text=task_card_text(r),
        reply_markup=km_review_nav(0, len(rows), r["id"], "flow"),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    FLOW_STATE[uid] = {"rows": rows, "idx": 0, "msg_id": m.message_id, "assignee": (name, tid)}

async def refresh_after_action(q, context):
    uid = str(q.from_user.id)
    mid = q.message.message_id
    # по message_id понимаем, откуда юзер нажал кнопку
    if uid in FLOW_STATE and FLOW_STATE[uid].get("msg_id") == mid:
        await send_flow_carousel_refresh(q, context)
        return
    if uid in CHECK_STATE and CHECK_STATE[uid].get("msg_id") == mid:
        await send_checktasks_carousel_refresh(q, context)
        return
    # запасной вариант — по флажку rv_origin, если его ставили
    origin = context.user_data.pop("rv_origin", None)
    if origin == "flow":
        await send_flow_carousel_refresh(q, context)
    else:
        await send_checktasks_carousel_refresh(q, context)

def is_overdue_task(t) -> bool:
    """Просрочена ли задача (по локальному TZ): deadline < сегодня и статус open/in_progress."""
    try:
        if (t["status"] not in ("open", "in_progress")):
            return False
        dl = (t["deadline"] or "").strip()
        if not dl:
            return False
        d = datetime.strptime(dl, "%Y-%m-%d").date()
        today = datetime.now(TZINFO).date()
        return d < today
    except Exception:
        return False

# --------------------------------------------------------------------------------
# Команды
# --------------------------------------------------------------------------------
async def mytasks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    rows = find_open_tasks_for_user(uid)
    if not rows:
        await update.message.reply_text("У тебя нет открытых задач. Еееей 🎉")
        return

    # Разделяем: просроченные → впереди, затем остальные (с сортировкой по дате)
    overdue = [r for r in rows if is_overdue_task(r)]
    upcoming = [r for r in rows if not is_overdue_task(r)]

    def _key_deadline(t):
        dl = (t["deadline"] or "").strip()
        return (datetime.max.date() if not dl else datetime.strptime(dl, "%Y-%m-%d").date())

    overdue.sort(key=_key_deadline)   # более ранние (давно просроченные) выше
    upcoming.sort(key=_key_deadline)  # ближайшие дедлайны выше

    ordered = overdue + upcoming

    # маленькая шапка-резюме
    if overdue:
        await update.message.reply_text(f"У тебя {len(ordered)} задач(и), из них просрочено: {len(overdue)} ⛔️")

    MYTASK_STATE[uid] = {"rows": ordered, "idx": 0, "msg_id": None}
    r = ordered[0]
    m = await update.message.reply_text(
        task_card_text(r),
        reply_markup=km_mytask_nav(0, len(ordered), r["id"]),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    MYTASK_STATE[uid]["msg_id"] = m.message_id

async def newtask_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text("Создаём в личке, ок? Напиши /newtask здесь.")
        return ConversationHandler.END
    context.user_data["in_newtask"] = True  # 👈 ставим флаг мастера создания
    await update.message.reply_text("Опиши задачу (одним сообщением).")
    return NT_TEXT

async def nt_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["nt_task_text"] = (update.message.text or "").strip()
    await update.message.reply_text("Кому назначить?", reply_markup=km_pick_assignee("nt_pick"))
    return NT_ASSIGNEE

async def nt_pick_assignee_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    _, tid = q.data.split(":", 1)
    # найдём имя по tid
    name = None
    for n, t in list_unique_assignees():
        if str(t) == str(tid):
            name = n
            break
    if not name:
        await q.edit_message_text("Не нашёл такого исполнителя. Попробуй ещё раз.")
        return
    context.user_data["nt_assignee_name"] = name
    context.user_data["nt_assignee_tid"]  = tid
    await q.edit_message_text(f"Исполнитель: {name}")
    await q.message.reply_text(
        "Это важная задача?\n\nУ важной задачи нельзя будет подвинуть дедлайн, у обычной — можно",
        reply_markup=KM([
            [B("Да", callback_data="nt_pr:high"), B("Нет", callback_data="nt_pr:normal")]
        ])
    )
    return NT_PRIORITY

async def nt_pick_priority_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    pr = q.data.split(":",1)[1]
    context.user_data["nt_priority"] = "high" if pr=="high" else "normal"
    await q.edit_message_text("Важность: " + ("🔥 важная" if pr=="high" else "обычная"))
    await q.message.reply_text(
        "Дедлайн? (Любой формат: YYYY-MM-DD / «завтра» / «сегодня»)",
        reply_markup=km_deadline_quick("newtask")
    )
    return NT_DEADLINE

async def nt_deadline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # принимаем дату только если действительно в мастере новой задачи
    if not context.user_data.get("in_newtask"):
        await update.message.reply_text("Сейчас не создаём задачу. Если нужна новая — командуй /newtask.")
        return ConversationHandler.END

    nd_raw = parse_any_date(update.message.text or "")
    nd = _ensure_future_or_today(nd_raw)
    if not nd:
        await update.message.reply_text("Дедлайн в прошлом нельзя. Дай 2025-09-01 / «завтра».")
        return NT_DEADLINE

    t  = context.user_data.get("nt_task_text","")
    an = context.user_data.get("nt_assignee_name")
    at = context.user_data.get("nt_assignee_tid")
    pr = context.user_data.get("nt_priority","normal")

    from db import add_or_update_assignee
    add_or_update_assignee(an, at)

    task_id = insert_task(t, an, at, nd, priority=pr, source="manual", status="proposed")
    await update.message.reply_text(f"Добавил черновик задачи. Проверь через /checktasks и отправь в работу.")

    # 👇 подчистим весь мастер, включая флаг
    for k in ("nt_task_text","nt_assignee_name","nt_assignee_tid","nt_priority","in_newtask"):
        context.user_data.pop(k, None)
    return ConversationHandler.END

async def nt_deadline_quick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # формат callback_data: dlq:newtask:0:<code>
    try:
        _, _, _, code = q.data.split(":", 3)
    except ValueError:
        code = "today"

    now = datetime.now(TZINFO)
    if code == "today":
        nd_raw = now.strftime("%Y-%m-%d")
    elif code == "tomorrow":
        nd_raw = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    elif code == "plus7":
        nd_raw = (now + timedelta(days=7)).strftime("%Y-%m-%d")
    elif code == "plus14":
        nd_raw = (now + timedelta(days=14)).strftime("%Y-%m-%d")
    else:
        nd_raw = now.strftime("%Y-%m-%d")

    nd = _ensure_future_or_today(nd_raw)
    if not nd:
        await q.message.reply_text("Дедлайн в прошлом нельзя. Дай 2025-09-01 / «завтра».")
        return NT_DEADLINE  # остаёмся в состоянии выбора дедлайна

    t  = context.user_data.get("nt_task_text","")
    an = context.user_data.get("nt_assignee_name")
    at = context.user_data.get("nt_assignee_tid")
    pr = context.user_data.get("nt_priority","normal")

    from db import add_or_update_assignee
    add_or_update_assignee(an, at)
    insert_task(t, an, at, nd, priority=pr, source="manual", status="proposed")

    await q.message.reply_text(f"Добавил черновик задачи. Проверь через /checktasks и отправь в работу.")

    # Чистим мастер и выходим из ConversationHandler
    for k in ("nt_task_text","nt_assignee_name","nt_assignee_tid","nt_priority","in_newtask"):
        context.user_data.pop(k, None)
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.username or "-"
    fname = update.effective_user.full_name or "-"
    logger.info("NEW /start: id=%s username=%s name=%s", uid, uname, fname)
    await update.message.reply_text("Салют. Я веду задачи. Упомяни меня в группе — я всё зафиксирую. Но сперва помощник проверит 😉 Забегай за новыми задачами!")

async def track_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await update.message.reply_text("Команда работает только в групповом чате.")
        return
    track_chat(chat.id, chat.title or "")
    set_last_chat_offset(chat.id, 0)
    await update.message.reply_text("Ок, теперь я отслеживаю этот чат. И не забудь отключить Privacy в @BotFather.")

# --------------------------------------------------------------------------------
# Поток ГРУПП: текст + голос → candidate → ВСЕГДА proposed → в /checktasks
# --------------------------------------------------------------------------------

async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_USERNAME
    msg: Message = update.message
    if not msg or not msg.text:
        return
    chat = msg.chat
    text = msg.text or ""
    MESSAGE_BUFFER.setdefault(str(chat.id), []).append((msg.message_id, msg.from_user.username or "",  msg.from_user.full_name, text, msg.date))
    set_last_chat_offset(chat.id, msg.message_id)

    if not BOT_USERNAME:
        me = await context.bot.get_me()
        BOT_USERNAME = me.username or ""

    mentioned = (f"@{BOT_USERNAME}".lower() in text.lower()) or any(
        e.type == "mention" and (text[e.offset:e.offset+e.length] or "").lower() == f"@{BOT_USERNAME}".lower()
        for e in (msg.entities or [])
    )
    if not mentioned:
        return

    task_text = (msg.reply_to_message.text if msg.reply_to_message and msg.reply_to_message.text else
                 strip_bot_mention(text, BOT_USERNAME)).strip()
    if not task_text:
        await msg.reply_text("Не вижу текста задачи. Напиши словами, я туповат, но настырный.")
        return

    msg_link = _tg_message_link(chat.id, msg.message_id)

    names = get_assignee_name_list()
    llm = llm_route(
        task_text, names,
        author_username=msg.from_user.username or "—",
        message_date=msg.date.astimezone(TZINFO).strftime("%Y-%m-%d"),
        message_link=msg_link
    ) or {}

    pr = (llm.get("priority") or "normal") if llm else extract_priority(task_text)
    dl = (llm.get("deadline") or None)    if llm else extract_deadline(task_text)

    assignee_name = None; assignee_tid = None
    if llm.get("assignee"):
        for n, tid in list_unique_assignees():
            if n == llm["assignee"]:
                assignee_name, assignee_tid = n, tid
                break
    if not assignee_name:
        cand, _ = detect_assignee(task_text, names)
        if cand:
            for n, tid in list_unique_assignees():
                if n == cand:
                    assignee_name, assignee_tid = n, tid
                    break

    logger.info("MENTION: creating proposed; pr=%s dl=%s assignee=%s text=%r",
            pr, dl, assignee_name, task_text)
    desc = (llm.get("description") or task_text).strip()
    desc = iso_to_human_in_text(desc)
    task_id = insert_task(
        desc, assignee_name or "", assignee_tid or "", dl or "",
        priority=pr, source="mention",
        source_chat_id=str(chat.id), source_message_id=msg.message_id,
        status="proposed", link=(llm.get("source_link") or msg_link or "")
    )

    await msg.reply_text("Задача зафиксирована ✅")

    pr_h = "Важная 🔥" if pr == "high" else "Обычная"
    link_line = f"\n🔗 Оригинал: {msg_link}" if msg_link else ""
    assignee_line = assignee_name or "—"
    if assignee_tid:
        _nick = get_nickname_by_tid(assignee_tid)
        if _nick:
            if not _nick.startswith("@"):
                _nick = "@" + _nick
            assignee_line = f"{assignee_line} ({_nick})"
    await notify_assistants(
        context,
        "Обнаружена задача — нужно подтвердить\n\n"
        f"🧩 Описание: {h(desc)}\n"
        f"🤡 Исполнитель: {assignee_name or '—'}\n"
        f"📅 Дедлайн: {fmt_date_human(dl)}\n"
        f"❗️ Приоритет: {pr_h}\n"   # ← добавили \n
        f"ID: #{task_id}"
        f"{link_line}\n\n"
        "Введите команду /checktasks, чтобы подтвердить и отправить в работу"
    )

async def on_group_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.message
    if not msg:
        return
    if update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    chat = msg.chat
    file_id = None
    if msg.voice:
        file_id = msg.voice.file_id
    elif msg.audio:
        file_id = msg.audio.file_id
    else:
        file_id = None
    if not file_id:
        return

    uname = msg.from_user.username or ""      # может быть пусто у пользователя без @username
    fname = msg.from_user.full_name or ""

    VOICE_BUFFER.setdefault(str(chat.id), []).append(
        (msg.message_id, uname, fname, file_id, msg.date)
)

# --------------------------------------------------------------------------------
# /checktasks — карусель всех proposed
# --------------------------------------------------------------------------------

def _ensure_check_state(user_id: str, rows):
    CHECK_STATE[user_id] = {"rows": list(rows), "idx": 0, "msg_id": None}

async def checktasks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text("Эй, давай в личке. Введи /checktasks ещё раз здесь, ок?")
        return

    uid = str(update.effective_user.id)
    if uid not in ALLOWED_FLOW_VIEWERS:
        await update.message.reply_text("Не-а. Доступ к ревью только у шефа и помощника.")
        return

    rows = fetch_proposed_tasks(100)
    if not rows:
        await update.message.reply_text("Ничего не ждёт апрува. Пусто как в холодильнике.")
        return

    _ensure_check_state(uid, rows)
    r = rows[0]
    m = await update.message.reply_text(
        task_card_text(r),
        reply_markup=km_review_nav(0, len(rows), r["id"], "check")
    )
    CHECK_STATE[uid]["msg_id"] = m.message_id

# --------------------------------------------------------------------------------
# /currentflow — выбрать ассайни → карусель всех его open/in_progress
# --------------------------------------------------------------------------------

async def currentflow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text("Чпок — давай в личке. Введи /currentflow ещё раз тут.")
        return
    if str(update.effective_user.id) not in ALLOWED_FLOW_VIEWERS:
        await update.message.reply_text("Не-а. Доступ к потоку только у шефа и помощника.")
        return
    rows = [(n, tid) for n, tid in list_unique_assignees() if str(tid or "").strip()]
    if not rows:
        await update.message.reply_text("Список исполнителей пуст. Печаль.")
        return
    kb = [[B(n, callback_data=f"flow_pick:{tid}")] for n, tid in rows]
    await update.message.reply_text("По кому смотрим поток?", reply_markup=KM(kb))

# --------------------------------------------------------------------------------
# /report — выгрузка CSV
# --------------------------------------------------------------------------------
async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if uid not in ALLOWED_FLOW_VIEWERS:
        await update.message.reply_text("Не-а. Доступ к отчётам только у шефа и помощника.")
        return

    rows = get_all_tasks()

    def yesno(v: bool) -> str:
        return "Да" if v else "Нет"

    buf = io.StringIO()
    
    writer = csv.writer(buf, delimiter=";", lineterminator="\r\n")

    writer.writerow([
        "ID", "Текст задачи", "Статус", "Назначен исполнитель?", "Исполнитель", "Дедлайн",
        "Важность", "Были ли переназначения?", "С кого на кого ?",
        "Был ли перенос?", "Даты переносов и кем"
    ])

    for r in rows:
        assigned = bool((r["assignee"] or "").strip() and (r["telegram_id"] or "").strip())

        # Переназначения
        reas = get_reassignments_for_task(r["id"])
        had_reassign = len(reas) > 0
        reassign_strs = []
        for x in reas:
            try:
                dt = datetime.fromisoformat((x["at"] or "").replace("Z", "+00:00")).astimezone(TZINFO)
                dstr = dt.strftime("%d.%m.%Y")
            except Exception:
                dstr = ""
            x_keys = x.keys() if hasattr(x, "keys") else []
            by_who = x["by_who"] if ("by_who" in x_keys and (x["by_who"] or "").strip()) else ""
            by = f" (Кем: {by_who})" if by_who else ""
            reassign_strs.append(f"{x['old_assignee']} → {x['new_assignee']} {dstr}{by}")

        # Переносы дедлайнов
        dchs = get_deadline_changes_for_task(r["id"])
        had_postpone = len(dchs) > 0
        postpone_strs = []
        for d in dchs:
            d_keys = d.keys() if hasattr(d, "keys") else []
            by_who = d["by_who"] if ("by_who" in d_keys and (d["by_who"] or "").strip()) else ""
            by = f" (Кем: {by_who})" if by_who else ""
            postpone_strs.append(f"{(d['old_deadline'] or '—')} → {(d['new_deadline'] or '—')}{by}")

        assignee_name = (r["assignee"] or "").strip() or "—"

        writer.writerow([
            r["id"], r["task"], r["status"], yesno(assigned), assignee_name, (r["deadline"] or ""),
            (r["priority"] or "normal"), yesno(had_reassign), "; ".join(reassign_strs) or "—",
            yesno(had_postpone), "; ".join(postpone_strs) or "—"
        ])

    data = buf.getvalue().encode("utf-8-sig")
    filename = f"tasks_report_{datetime.now(TZINFO).strftime('%Y%m%d_%H%M')}.csv"

    await update.message.reply_document(
        document=InputFile(io.BytesIO(data), filename=filename),
        caption="Готов отчёт."
    )

async def outdated_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today_str = datetime.now(TZINFO).strftime("%Y-%m-%d")
    rows = get_overdue_open_tasks(today_str)
    if not rows:
        await update.message.reply_text("Просроченных задач нет. Красота!")
        return
    

    lines = ["<b>Все просроченные задачи в работе:</b>", ""]
    for t in rows:
        dl = (t["deadline"] or "").strip()
        assignee_disp = fmt_assignee_with_nick((t['assignee'] or '—'), t['telegram_id'])
        lines.append(
            f"ID: <code>{t['id']}</code>\n"
            f"Описание: {t['task']}\n"
            f"Исполнитель: {assignee_disp}\n"
            f"Дедлайн: {_fmt_date(dl)}\n"
        )

    text = "\n".join(lines)
    # отправим построчно если слишком длинно
    if len(text) < 3900:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        chunk = []
        size = 0
        for block in lines:
            b = block + "\n"
            if size + len(b) > 3500:
                await update.message.reply_text("\n".join(chunk), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                chunk, size = [], 0
            chunk.append(block); size += len(b)
        if chunk:
            await update.message.reply_text("\n".join(chunk), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

def _fmt_date(d: str) -> str:
    if not d: return "—"
    try:
        y,m,dd = map(int, d.split("-"))
        months = ["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"]
        return f"{dd} {months[m-1]} {y}"
    except Exception:
        return d


# --------------------------------------------------------------------------------
# Общий обработчик коллбеков
# --------------------------------------------------------------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    uid = str(q.from_user.id)

    # быстрые дедлайны
    if data.startswith("dlq:"):
        # dlq:<context>:<task_id_or_0>:<code>
        _, ctx_tag, sid, code = data.split(":", 3)
        now = datetime.now(TZINFO)

        if code == "today":
            nd_raw = now.strftime("%Y-%m-%d")
        elif code == "tomorrow":
            nd_raw = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        elif code == "plus7":
            nd_raw = (now + timedelta(days=7)).strftime("%Y-%m-%d")
        elif code == "plus14":
            nd_raw = (now + timedelta(days=14)).strftime("%Y-%m-%d")
        else:
            nd_raw = now.strftime("%Y-%m-%d")

        nd = _ensure_future_or_today(nd_raw)
        if not nd:
            await q.message.reply_text("Дата уже в прошлом — выбери заново или введи реальную будущую дату.")
            return

        # newtask обрабатывается внутри ConversationHandler (nt_deadline_quick_cb), сюда не попадает
        if ctx_tag == "newtask":
            return

        # 👇 ВЕРНИ ЭТУ СТРОКУ — нужна для rv/postpone/take
        task_id = int(sid)

        if ctx_tag == "rv":
            set_task_deadline(task_id, nd, mark_postponed=False)
            context.user_data["rv_step"] = "priority_confirm"
            await q.message.reply_text("Это важная задача?", reply_markup=KM([
                [B("Да", callback_data="rv:prio_high"), B("Нет", callback_data="rv:prio_norm")]
            ]))
            return

        if ctx_tag == "postpone":
            if (get_priority(task_id) or "normal") == "high":
                await q.message.reply_text("Это 🔥 ВАЖНАЯ задача — перенос дедлайна запрещён.")
                return
            set_task_deadline(task_id, nd, mark_postponed=True, by_who=q.from_user.full_name or "—")
            await q.message.reply_text(f"Перенёс на {fmt_date_human(nd)}. Но я вернусь 😉")
            return

        if ctx_tag == "take":
            set_task_deadline(task_id, nd, mark_postponed=False)
            await q.message.reply_text(f"Дедлайн поставлен: {fmt_date_human(nd)}. Удачи!")
            return

        return
        
    if data.startswith("nt_pick:") or data.startswith("nt_pr:"):
        return
    if data in ("nav:noop", "mt_nav:noop"):
        return

    # /mytasks: навигация
    if data.startswith("mt_nav:"):
        st = MYTASK_STATE.get(uid)
        if not st or not st.get("rows"):
            await q.answer("Список пуст.")
            return
        try:
            idx = int(data.split(":", 1)[1])
        except Exception:
            idx = st["idx"]
        idx = max(0, min(idx, len(st["rows"]) - 1))
        st["idx"] = idx
        r = st["rows"][idx]
        await safe_edit_text(q.message, task_card_text(r), reply_markup=km_mytask_nav(idx, len(st["rows"]), r["id"]))
        return

    # /currentflow: выбор человека
    if data.startswith("flow_pick:"):
        _, tid = data.split(":", 1)
        # найдём имя по tid
        name = None
        for n, t in list_unique_assignees():
            if str(t) == str(tid):
                name = n
                break
        rows = get_tasks_by_tid_openlike(tid)
        if not rows:
            await safe_edit_text(q.message, f"{name or 'Исполнитель'}: нет открытых задач.")
            return
        FLOW_STATE[uid] = {"rows": rows, "idx": 0, "msg_id": q.message.message_id, "assignee": (name or "", tid)}
        r = rows[0]
        await safe_edit_text(q.message, task_card_text(r), reply_markup=km_review_nav(0, len(rows), r["id"], "flow"))
        return

    # Карусели: навигация
    if data.startswith("nav:flow:"):
        idx_str = data.split(":", 2)[2]
        st = FLOW_STATE.get(uid)
        if not st or not st.get("rows"): return
        try:
            idx = int(idx_str)
        except Exception:
            idx = st["idx"]
        idx = max(0, min(idx, len(st["rows"]) - 1))
        st["idx"] = idx
        r = st["rows"][idx]
        await safe_edit_text(q.message, task_card_text(r), reply_markup=km_review_nav(idx, len(st["rows"]), r["id"], "flow"))
        return

    if data.startswith("nav:check:"):
        idx_str = data.split(":", 2)[2]
        st = CHECK_STATE.get(uid)
        if not st or not st.get("rows"): return
        try:
            idx = int(idx_str)
        except Exception:
            idx = st["idx"]
        idx = max(0, min(idx, len(st["rows"]) - 1))
        st["idx"] = idx
        r = st["rows"][idx]
        await safe_edit_text(q.message, task_card_text(r), reply_markup=km_review_nav(idx, len(st["rows"]), r["id"], "check"))

        return

    # Действия в ревью-каруселях
    if data.startswith("act:"):
        _, action, sid = data.split(":", 2)
        task_id = int(sid)

        if action == "approve":
            await approve_and_start(task_id, q, context)
            return

        if action == "cancel":
            for key in ("await_deadline", "await_deadline_take"):
                context.user_data.pop(key, None)
            context.user_data.pop("rv_step", None)

            # запомним, чтобы при "Своим текстом" принять сообщение
            context.user_data["await_reason"] = True
            context.user_data["rv_cancel_task"] = task_id
            src = "flow" if (uid in FLOW_STATE and FLOW_STATE[uid].get("msg_id") == q.message.message_id) else "check"
            context.user_data["rv_origin"] = src

            kb = KM([
                [B("Не задача",      callback_data=f"cr:{task_id}:not_task")],
                [B("Уже сделано",    callback_data=f"cr:{task_id}:done")],
                [B("Не актуально",   callback_data=f"cr:{task_id}:not_relevant")],
                [B("✍️ Своим текстом", callback_data=f"cr:{task_id}:other")]
            ])
            await q.message.reply_text("Почему снимаем задачу?", reply_markup=kb)
            return


        if action == "edit":
            context.user_data["rv_edit_task"] = task_id
            context.user_data["rv_step"] = "desc_confirm"
            t = get_task(task_id)
            desc = t["task"]
            await q.message.reply_text(
                "Описание задачи верно?\n\n" + desc,
                reply_markup=KM([
                    [B("Да", callback_data="rv:desc_ok"), B("Нет", callback_data="rv:desc_no")]
                ])
            )
            return

        if action == "reassign":
            rows = [(n, tid) for n, tid in list_unique_assignees() if str(tid or "").strip()]
            kb = [[B(n, callback_data=f"rv_reassign_to:{task_id}:{tid}")] for n, tid in rows]
            src = "flow" if (uid in FLOW_STATE and FLOW_STATE[uid].get("msg_id") == q.message.message_id) else "check"
            context.user_data["rv_origin"] = src
            await q.message.reply_text("Кому переназначаем?", reply_markup=KM(kb))
            return

    # Быстрые причины отмены
    if data.startswith("cr:"):
        _, sid, code = data.split(":", 2)
        task_id = int(sid)
        reason_map = {
            "not_task":     "Не задача",
            "done":         "Уже сделано",
            "not_relevant": "Не актуально",
        }

        if code == "other":
            # остаёмся ждать текст, флаги уже выставлены выше
            await q.message.reply_text("Ок, напиши короткий комментарий — я сохраню в карточке.")
            return

        # моментально отменяем с типовой причиной
        mark_cancelled(task_id, reason_map.get(code, "Не актуально"))
        # подчистим режим ожидания комментария, если был
        context.user_data.pop("await_reason", None)
        context.user_data.pop("rv_cancel_task", None)
        origin = context.user_data.pop("rv_origin", None)

        await q.message.reply_text("Ок, снял с повестки. Причину записал. ✅")
        # обновим нужную карусель
        class Dummy: pass
        d = Dummy(); d.message = q.message; d.from_user = q.from_user
        if origin == "flow":
            await send_flow_carousel_refresh(d, context)
        else:
            await send_checktasks_carousel_refresh(d, context)
        return

    # Мастер правок (пошагово)
    if data.startswith("rv:"):
        token = data.split(":", 1)[1]
        step = context.user_data.get("rv_step")
        task_id = context.user_data.get("rv_edit_task")

        if token == "deadline_ok" and step == "deadline_confirm":
            t = get_task(task_id)
            if not (t and (t["deadline"] or "").strip()):
                context.user_data["rv_step"] = "deadline_edit"
                await q.message.reply_text("Дедлайна нет. Введи дату (YYYY-MM-DD / «завтра»).")
                return
            context.user_data["rv_step"] = "priority_confirm"
            await q.message.reply_text("Это важная задача?", reply_markup=KM([
                [B("Да", callback_data="rv:prio_high"), B("Нет", callback_data="rv:prio_norm")]
            ]))
            return

        if token == "desc_no" and step == "desc_confirm":
            # 1) отдельным сообщением — старый текст (удобно копировать)
            t = get_task(task_id)
            await q.message.reply_text(t["task"] or "—")
            # 2) затем наша подсказка на ввод
            context.user_data["rv_step"] = "desc_edit"
            await q.message.reply_text("Введи новое описание сообщением.")
            return

        if token == "assignee_ok" and step == "assignee_confirm":
            t = get_task(task_id)
            cur_deadline = (t["deadline"] or "").strip()
            if not cur_deadline:
                context.user_data["rv_step"] = "deadline_edit"
                await q.message.reply_text(
                    "Дедлайна нет. Введи дату (YYYY-MM-DD / «завтра»).",
                    reply_markup=km_deadline_quick("rv", task_id)
                )
                return
            context.user_data["rv_step"] = "deadline_confirm"
            await q.message.reply_text(
                f"Дедлайн верен? (сейчас: {fmt_date_human(cur_deadline)})",
                reply_markup=KM([[B("Да", callback_data="rv:deadline_ok"),
                                B("Нет", callback_data="rv:deadline_no")]])
            )
            return

        if token == "assignee_no" and step == "assignee_confirm":
            context.user_data["rv_step"] = "assignee_pick"
            await q.message.reply_text("Выбери нового исполнителя:", reply_markup=km_pick_assignee("rv_pick"))
            return

        if token == "desc_ok" and step == "desc_confirm":
            context.user_data["rv_step"] = "assignee_confirm"
            t = get_task(task_id)
            cur_assignee = (t["assignee"] or "").strip()
            cur_assignee_disp = fmt_assignee_with_nick(cur_assignee, t["telegram_id"])
            if not cur_assignee:
                # нет исполнителя — сразу выбор, без «Да/Нет»
                context.user_data["rv_step"] = "assignee_pick"
                await q.message.reply_text("Исполнитель не указан. Выбери исполнителя:",
                                        reply_markup=km_pick_assignee("rv_pick"))
                return
            await q.message.reply_text(
                f"Исполнитель верен? (сейчас: {cur_assignee_disp})",
                reply_markup=KM([
                    [B("Да", callback_data="rv:assignee_ok"), B("Нет", callback_data="rv:assignee_no")]
                ])
            )
            return

        if token == "deadline_no" and step == "deadline_confirm":
            context.user_data["rv_step"] = "deadline_edit"
            await q.message.reply_text(
                "Введи новый дедлайн (например: 2025-09-01, «завтра», «к пятнице», «до конца недели»).",
                reply_markup=km_deadline_quick("rv", task_id)
            )
            return

        if token in ("prio_high", "prio_norm") and step == "priority_confirm":
            pr = "high" if token == "prio_high" else "normal"
            set_task_priority(task_id, pr)
            context.user_data.pop("rv_step", None)
            await approve_and_start(task_id, q, context)
            # карусель обновим после апрува
            return

    # Выбор нового исполнителя в мастере правок
    if data.startswith("rv_pick:"):
        # формат: rv_pick:<tid>
        _, tid = data.split(":", 1)
        # найти имя по tid
        name = None
        for n, t in list_unique_assignees():
            if str(t) == str(tid):
                name = n
                break
        if not name:
            await q.message.reply_text("Не нашёл такого исполнителя.")
            return

        update_task_assignment(int(context.user_data.get("rv_edit_task")), name, tid, by_who=q.from_user.full_name or "—")
        t2 = get_task(int(context.user_data.get("rv_edit_task")))
        cur_deadline = (t2["deadline"] or "").strip()
        disp = fmt_assignee_with_nick(name, tid)
        if not cur_deadline:
            context.user_data["rv_step"] = "deadline_edit"
            await q.message.reply_text(
                f"Исполнитель теперь: {disp}\nДедлайна нет. Введи дату (YYYY-MM-DD / «завтра»).",
                reply_markup=km_deadline_quick("rv", int(context.user_data.get("rv_edit_task")))
            )
        else:
            context.user_data["rv_step"] = "deadline_confirm"
            await q.message.reply_text(
                f"Исполнитель теперь: {disp}\nДедлайн верен? (сейчас: {fmt_date_human(cur_deadline)})",
                reply_markup=KM([[B("Да", callback_data="rv:deadline_ok"),
                                B("Нет", callback_data="rv:deadline_no")]])
            )
        return

    # Переназначение из ревью (мгновенно, с отбивками)
    if data.startswith("rv_reassign_to:"):
        # формат: rv_reassign_to:<task_id>:<tid>
        _, sid, tid = data.split(":", 2)
        task_id = int(sid)
        t_before = get_task(task_id)
        old_tid = t_before["telegram_id"]
        who = q.from_user.full_name or "Кто-то умный"

        # найти имя по tid
        new_name = None
        for n, t in list_unique_assignees():
            if str(t) == str(tid):
                new_name = n
                break
        if not new_name:
            await q.message.reply_text("Не нашёл такого исполнителя.")
            return

        update_task_assignment(task_id, new_name, tid, by_who=who)
        t_after = get_task(task_id)

        if old_tid and str(old_tid) != str(tid):
            try:
                await context.bot.send_message(
                    chat_id=str(old_tid),
                    text=f"Задача «{t_after['task']}» больше не на тебе. Новый исполнитель: {new_name}."
                )
            except Exception:
                pass

        try:
            await notify_new_assignment(context, to_tid=tid, task=t_after, who=who)
        except Exception:
            pass

        await q.message.reply_text(f"Готово. Переназначено на {new_name}.")
        await refresh_after_action(q, context)
        return

    # Исполнительские кнопки на карточке
    if data.startswith("take:"):
        task_id = int(data.split(":")[1])
        set_task_status(task_id, "in_progress")
        await q.edit_message_reply_markup(reply_markup=None)
        t = get_task(task_id)
        if not (t and (t["deadline"] or "").strip()):
            await q.message.reply_text(
                "Прими дедлайн: введи дату (YYYY-MM-DD / «завтра») или нажми кнопку.",
                reply_markup=km_deadline_quick("take", task_id)
            )
            context.user_data["await_deadline_take"] = task_id
        else:
            await q.message.reply_text("Записал: задача в работе 😉")
        return

    if data.startswith("reassign:"):
        task_id = int(data.split(":")[1])
        buttons = [[B(n, callback_data=f"reassign_to:{task_id}:{tid}")]
           for n, tid in list_unique_assignees() if str(tid or "").strip()]
        await q.message.reply_text("Кому перекидываем?", reply_markup=KM(buttons))
        return

    if data.startswith("reassign_to:"):
        # формат: reassign_to:<task_id>:<tid>
        _, sid, tid = data.split(":", 2)
        task_id = int(sid)
        t_before = get_task(task_id)
        old_tid = t_before["telegram_id"]
        who = q.from_user.full_name or "Коллега"

        # найти имя по tid
        new_name = None
        for n, t in list_unique_assignees():
            if str(t) == str(tid):
                new_name = n
                break
        if not new_name:
            await q.message.reply_text("Не нашёл такого исполнителя.")
            return

        update_task_assignment(task_id, new_name, tid, by_who=who)
        t_after = get_task(task_id)

        if old_tid and str(old_tid) != str(tid):
            try:
                await context.bot.send_message(
                    chat_id=str(old_tid),
                    text=f"Задачу «{t_after['task']}» сняли с тебя. Новый исполнитель: {new_name}."
                )
            except Exception:
                pass

        try:
            await notify_new_assignment(context, to_tid=tid, task=t_after, who=who)
        except Exception:
            pass

        await q.message.reply_text(f"Готово. Переназначено на {new_name}.")
        await refresh_after_action(q, context)
        return

    # ---------- НОВОЕ: подтверждение наличия файла при закрытии ----------
    if data.startswith("done:") or data.startswith("mt_done:"):
        task_id = int(data.split(":")[1])
        kb = KM([[B("📎 Да, прикреплю", callback_data=f"proof_yes:{task_id}")],
                 [B("Без файла", callback_data=f"proof_no:{task_id}")]])
        await q.message.reply_text("Есть ли доказательство (файл/фото/видео/аудио)?", reply_markup=kb)
        return

    if data.startswith("proof_yes:"):
        task_id = int(data.split(":")[1])
        context.user_data["await_proof_for_task"] = task_id
        await q.message.reply_text("Пришли файл-подтверждение, если есть (док/фото/видео/аудио; писать сообщение не нужно).")
        return

    if data.startswith("proof_no:"):
        task_id = int(data.split(":")[1])
        # Закрываем без файла
        set_task_status(task_id, "done")
        t = get_task(task_id)
        await q.message.reply_text("Ну ладно, но старайся фиксировать свои успехи файлами 😉")
        await broadcast_task_closed(context, t, performer_name=q.from_user.full_name or None, with_file_first_msg_id=None, src_chat=None)
        return

    if data.startswith("cant_do:") or data.startswith("mt_cant:"):
        task_id = int(data.split(":")[1])
        if (get_priority(task_id) or "normal") == "high":
            await q.message.reply_text("Это 🔥 ВАЖНАЯ задача — перенос дедлайна запрещён.")
            return
        context.user_data["await_deadline"] = task_id
        await q.message.reply_text(
            "Когда перенесём? Дай дату (2025-09-01 / «завтра» / «к пятнице»…) или нажми кнопку.",
            reply_markup=km_deadline_quick("postpone", task_id)
        )
        return

ALLOWED_WIPE = {str(VADIM_CHAT_ID), *map(str, ASSISTANT_CHAT_IDS)}

async def wipe_tasks_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) not in ALLOWED_WIPE:
        await update.message.reply_text("Не-а. Это только для шефа и помощника.")
        return
    await update.message.reply_text("ВНИМАНИЕ! Удалю ВСЕ задачи в статусах proposed/open/in_progress.\nПодтверди командой: /wipe_tasks_confirm")

async def wipe_tasks_confirm_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) not in ALLOWED_WIPE:
        await update.message.reply_text("Не-а. Это только для шефа и помощника.")
        return
    from db import _wipe_open_like
    n = _wipe_open_like()
    await update.message.reply_text(f"Готово. Удалено задач: {n} (proposed/open/in_progress).")

# --------------------------------------------------------------------------------
# Тексты в ЛС: мастер правок, дедлайны, причина отмены, подтверждение файлом
# --------------------------------------------------------------------------------

async def on_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    text = msg.text or ""

    # 0) ПРИОРИТЕТ: причина отмены после «Неактуально»
    if context.user_data.pop("await_reason", False) or ("rv_cancel_task" in context.user_data):
        from db import mark_cancelled
        task_id = int(context.user_data.pop("rv_cancel_task", 0) or 0)
        origin = context.user_data.pop("rv_origin", None)

        if task_id:
            mark_cancelled(task_id, (text or "").strip())
            await msg.reply_text("Ок, снял с повестки. Причину записал. ✅")
            # обновим нужную карусель
            class Dummy: pass
            d = Dummy(); d.message = msg; d.from_user = update.effective_user
            if origin == "flow":
                await send_flow_carousel_refresh(d, context)
            else:
                await send_checktasks_carousel_refresh(d, context)
        else:
            await msg.reply_text("Не нашёл задачу для отмены. Открой /checktasks и попробуй ещё раз.")
        return

    if "rv_cancel_task" in context.user_data:
        from db import mark_cancelled
        task_id = int(context.user_data.pop("rv_cancel_task"))
        origin = context.user_data.pop("rv_origin", None)
        mark_cancelled(task_id, (text or "").strip())
        await msg.reply_text("Ок, снял с повестки. Причину записал. ✅")
        # обновим нужную карусель
        class Dummy: pass
        d = Dummy(); d.message = msg; d.from_user = update.effective_user
        if origin == "flow":
            await send_flow_carousel_refresh(d, context)
        else:
            await send_checktasks_carousel_refresh(d, context)
        return

    if context.user_data.get("rv_step") == "desc_edit":
        task_id = int(context.user_data.get("rv_edit_task"))
        # ✅ приводим ISO-даты вида 2025-09-11 к «11 Сентября 2025»
        text_clean = iso_to_human_in_text((text or "").strip())
        set_task_text(task_id, text_clean)
        t = get_task(task_id)
        cur_assignee = (t["assignee"] or "").strip()
        if not cur_assignee:
            context.user_data["rv_step"] = "assignee_pick"
            await msg.reply_text("Исполнитель не указан. Выбери исполнителя:",
                                reply_markup=km_pick_assignee("rv_pick"))
        else:
            context.user_data["rv_step"] = "assignee_confirm"
            await msg.reply_text(
                f"Принял. Исполнитель верен? (сейчас: {cur_assignee})",
                reply_markup=KM([[B("Да", callback_data="rv:assignee_ok"),
                                B("Нет", callback_data="rv:assignee_no")]])
            )
        return

    if context.user_data.get("rv_step") == "deadline_edit":
        task_id = int(context.user_data.get("rv_edit_task"))
        nd_raw = parse_any_date(text)
        nd = _ensure_future_or_today(nd_raw)
        if not nd:
            await msg.reply_text("Дедлайн в прошлом нельзя. Дай вид 2025-09-01 или «завтра».")
            return
        set_task_deadline(task_id, nd, mark_postponed=False)
        context.user_data["rv_step"] = "priority_confirm"
        await msg.reply_text("Это важная задача?", reply_markup=KM([
            [B("Да", callback_data="rv:prio_high"), B("Нет", callback_data="rv:prio_norm")]
        ]))
        return


    if "await_deadline" in context.user_data:
        task_id = int(context.user_data.pop("await_deadline"))
        nd_raw = parse_any_date(text)
        nd = _ensure_future_or_today(nd_raw)
        if not nd:
            await msg.reply_text("Нельзя ставить дату в прошлом. Дай 2025-09-01 / «завтра».")
            return
        set_task_deadline(task_id, nd, mark_postponed=True, by_who=update.effective_user.full_name or "—")
        await msg.reply_text(f"Перенёс на {fmt_date_human(nd)}. Но я вернусь 😉")
        return

    if "await_deadline_take" in context.user_data:
        task_id = int(context.user_data.pop("await_deadline_take"))
        nd_raw = parse_any_date(text)
        nd = _ensure_future_or_today(nd_raw)
        if not nd:
            await msg.reply_text("Нельзя ставить дату в прошлом. Дай 2025-09-01 / «завтра».")
            return
        set_task_deadline(task_id, nd, mark_postponed=False)
        await msg.reply_text(f"Дедлайн поставлен: {fmt_date_human(nd)}. Удачи!")
        return

async def on_private_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "await_proof_for_task" not in context.user_data:
        return
    task_id = int(context.user_data.pop("await_proof_for_task"))
    t = get_task(task_id)
    performer_name = update.effective_user.full_name or None

    # 1) Сначала публикуем красивую карточку «закрыто»
    await broadcast_task_closed(context, t, performer_name=performer_name, with_file_first_msg_id=None, src_chat=None)

    # 2) Затем пересылаем файл шефу и всем ассистентам
    try:
        src_chat = update.effective_chat.id
        src_msg  = update.message.message_id
        await context.bot.copy_message(chat_id=VADIM_CHAT_ID, from_chat_id=src_chat, message_id=src_msg)
        await copy_to_assistants(context, from_chat_id=src_chat, message_id=src_msg)
    except Exception:
        pass

    set_task_status(task_id, "done")
    await update.message.reply_text("Принял файл, перекинул начальству. Задачу закрыл. 🧷")

# --------------------------------------------------------------------------------
# ВЕЧЕРНИЙ ДАЙДЖЕСТ (из буферов) + ежедневный файл-отчёт
# --------------------------------------------------------------------------------
async def testdigest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await evening_digest(context)
    await update.message.reply_text("Прогнал вечерний дайджест вручную.")

async def evening_digest(context: ContextTypes.DEFAULT_TYPE):
    # 1) Голосовые → текст
    for chat_id, items in list(VOICE_BUFFER.items()):
        for (mid, uname, fname, file_id, dt) in items:
            try:
                text = await transcribe_telegram_file(context.bot, file_id)
            except Exception:
                text = ""
            if text:
                # сохраняем уже в «текстовый» буфер с username/full_name
                MESSAGE_BUFFER.setdefault(chat_id, []).append((mid, uname, fname, text, dt))
                logger.info(f"Voice transcribed from chat {chat_id} by @{uname or '—'} ({fname}): {text}")
        VOICE_BUFFER[chat_id] = []

    # 2) LLM-фильтр
    created = 0
    for chat_id, items in list(MESSAGE_BUFFER.items()):
        for (mid, username, full_name, text, dt) in items:
            if not text:
                continue

            msg_link = _tg_message_link(chat_id, mid)
            names = get_assignee_name_list()
            llm = llm_route(
                text, names,
                author_username=(username or "—"),
                message_date=dt.astimezone(TZINFO).strftime("%Y-%m-%d"),
                message_link=msg_link
            ) or {}

            is_task = bool(llm.get("looks_like_task")) if llm else looks_like_task(text)
            pr      = (llm.get("priority") or "normal") if llm else extract_priority(text)
            dl      = (llm.get("deadline") or None)     if llm else extract_deadline(text)
            ass_llm = llm.get("assignee") if llm else None

            logger.info(
                "DIGEST LLM: is_task=%s conf=%s pr=%s dl=%s assignee=%s text=%r",
                is_task, (llm.get("confidence") if llm else None), pr, dl, ass_llm, text
            )

            if not is_task:
                continue

            assignee_name, assignee_tid = None, None
            if ass_llm:
                for n, tid in list_unique_assignees():
                    if n == ass_llm:
                        assignee_name, assignee_tid = n, tid
                        break

            desc = (llm.get("description") or text).strip()
            desc = iso_to_human_in_text(desc)

            task_id = insert_task(
                desc, assignee_name or "", assignee_tid or "", dl or "",
                priority=pr, source="digest", source_chat_id=str(chat_id), source_message_id=mid,
                status="proposed", link=(llm.get("source_link") or msg_link or "")
            )
            logger.info("DIGEST: created proposed task_id=%s chat=%s msg=%s desc=%r", task_id, chat_id, mid, desc)
            created += 1

            # уведомляем ассистентов
            pr_h = "Важная 🔥" if pr == "high" else "Обычная"
            assignee_line = fmt_assignee_with_nick(assignee_name or "—", assignee_tid)
            link_line = f"\n🔗 Оригинал: {msg_link}" if msg_link else ""

            await notify_assistants(
                context,
                "Обнаружена задача (вечерний разбор) — нужно подтвердить\n\n"
                f"🧩 Описание: {h(desc)}\n"
                f"🤡 Исполнитель: {assignee_line}\n"
                f"📅 Дедлайн: {fmt_date_human(dl)}\n"
                f"❗️ Приоритет: {pr_h}\n"
                f"ID: #{task_id}"
                f"{link_line}\n\n"
                "Введите команду /checktasks, чтобы подтвердить и отправить в работу"
            )

    # чистим буфер
    MESSAGE_BUFFER.clear()

    # короткий итог ассистентам и шефу
    summary = f"⏰ Вечерний разбор: найдено задач-кандидатов: {created}.\nОткрой /checktasks для подтверждения."
    try:
        await context.bot.send_message(chat_id=str(VADIM_CHAT_ID), text=summary)
    except Exception as e:
        logger.warning("Итог дня не дошёл до VADIM_CHAT_ID: %s", e)
    await notify_assistants(context, summary)




# --------------------------------------------------------------------------------
# Регистрация
# --------------------------------------------------------------------------------
async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Логируем всю трассу, не падаем
    logger.exception("Update error: %s", context.error)

async def nt_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for k in ("nt_task_text","nt_assignee_name","nt_assignee_tid","nt_priority","in_newtask"):
        context.user_data.pop(k, None)
    await update.message.reply_text("Окей, отменил создание.")
    return ConversationHandler.END


def build_app():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.COMMAND, first_touch_check),
        group=-1
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("track", track_cmd))
    app.add_handler(CommandHandler("checktasks", checktasks_cmd))
    app.add_handler(CommandHandler("currentflow", currentflow_cmd))
    app.add_handler(CommandHandler("mytasks", mytasks_cmd))
    app.add_handler(CommandHandler("testdigest", testdigest_cmd))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("outdated", outdated_cmd))


    newtask_conv = ConversationHandler(
        entry_points=[CommandHandler("newtask", newtask_cmd)],
        states={
            NT_TEXT:     [MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, nt_text)],
            NT_ASSIGNEE: [CallbackQueryHandler(nt_pick_assignee_cb, pattern=r"^nt_pick:")],
            NT_PRIORITY: [CallbackQueryHandler(nt_pick_priority_cb, pattern=r"^nt_pr:")],
            NT_DEADLINE: [
                MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, nt_deadline),
                CallbackQueryHandler(nt_deadline_quick_cb, pattern=r"^dlq:newtask:")
            ],
        },
        fallbacks=[CommandHandler("cancel", nt_cancel)],
    )
    app.add_handler(newtask_conv, group=0)

    app.add_handler(CallbackQueryHandler(on_callback), group=1)

    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_private_text))
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & (filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE),
        on_private_file
    ))

    app.add_handler(CommandHandler("wipe_tasks", wipe_tasks_cmd))
    app.add_handler(CommandHandler("wipe_tasks_confirm", wipe_tasks_confirm_cmd))

    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_group_text))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & (filters.VOICE | filters.AUDIO), on_group_voice))

    # Дайджест из буферов (LLM-классификация) — как было
    app.job_queue.run_daily(
        evening_digest,
        time=dtime(hour=WORK_END_HOUR, minute=0, tzinfo=TZINFO),
        days=(0,1,2,3,4)
    )

    
    app.add_error_handler(on_error)

    return app

def main():
    app = build_app()
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
