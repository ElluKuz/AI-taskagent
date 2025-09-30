# -*- coding: utf-8 -*-
import os, json, time, requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from scheduler import build_combined_pdf_report # XLSX больше не нужен

from app_config import TZ, BOT_TOKEN, VADIM_CHAT_ID, ASSISTANT_CHAT_IDS
from db import (
    count_open_like,
    count_closed_between,
    get_overdue_open_tasks,
    get_deadline_changes_between,
    get_tasks_due_on,
    get_task,
    get_nickname_by_tid,
)

TZINFO = ZoneInfo(TZ)
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
WEEKDAYS = {0,1,2,3,4}  # пн–пт

def _assignee_with_nick(name: str | None, tid: str | None) -> str:
    n = (name or "—").strip()
    nick = (get_nickname_by_tid(tid or "") or "").strip()
    if nick:
        if not nick.startswith("@"):
            nick = "@" + nick
        return f"{n} ({nick})"
    return n

def _fmt_date(d: str) -> str:
    if not d: return "—"
    try:
        y,m,dd = map(int, d.split("-"))
        months = ["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"]
        return f"{dd} {months[m-1]} {y}"
    except Exception:
        return d

def _send_document(chat_id: str, file_path: str, caption: str | None = None):
    try:
        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            data = {"chat_id": str(chat_id)}
            if caption:
                data["caption"] = caption
                data["parse_mode"] = "HTML"
            requests.post(f"{TG_API}/sendDocument", data=data, files=files, timeout=60)
    except Exception:
        pass

def _send(chat_id: str, text: str):
    payload = {"chat_id": str(chat_id), "text": text, "parse_mode":"HTML", "disable_web_page_preview": True}
    r = requests.post(f"{TG_API}/sendMessage", json=payload, timeout=20)
    if not r.ok and "message is too long" in (r.text or "").lower():
        # простая нарезка, если внезапно длинно
        parts, cur = [], ""
        for line in text.split("\n"):
            add = (line + "\n")
            if len(cur) + len(add) < 3800:
                cur += add
            else:
                parts.append(cur); cur = add
        if cur.strip():
            parts.append(cur)
        for i, p in enumerate(parts, 1):
            requests.post(
                f"{TG_API}/sendMessage",
                json={
                    "chat_id": str(chat_id),
                    "text": (f"(часть {i}/{len(parts)})\n\n" + p),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                },
                timeout=20
            )

def _prev_workday(now_local: datetime) -> datetime:
    d = now_local - timedelta(days=1)
    while d.weekday() not in WEEKDAYS:
        d -= timedelta(days=1)
    return d

def build_morning_summary(now_local: datetime) -> str:
    """
    Отчёт за пред. рабочий день (окно: 09:00 пред.раб.дня → 09:00 сегодня).
    Разделы:
      1) «Список просроченных задач за сутки» — те, у кого дедлайн был ВЧЕРА и задача осталась открытой.
      2) «Список переносов (вчера не успели и перенесли дедлайн)» — у кого старый дедлайн был ВЧЕРА и в окне его перенесли на сегодня/будущее.
    """
    # окно
    end_local   = now_local.replace(hour=9, minute=0, second=0, microsecond=0)
    start_local = _prev_workday(end_local).replace(hour=9, minute=0, second=0, microsecond=0)
    start_utc = start_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc   = end_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    today_str     = end_local.strftime("%Y-%m-%d")
    yesterday_str = (end_local - timedelta(days=1)).strftime("%Y-%m-%d")

    # метрики
    total_open_like  = count_open_like()                         # open + in_progress (срез на сейчас)
    closed_last_day  = count_closed_between(start_utc, end_utc)  # закрыты в окне
    overall_overdue  = get_overdue_open_tasks(today_str)         # все просроченные на сейчас
    overall_overdue_count = len(overall_overdue)

    # --- 2-я часть: переносы дедлайна «со вчера» вперёд в данном окне
    changes = get_deadline_changes_between(start_utc, end_utc)   # уже JOIN с tasks
    postponed_map = {}  # task_id -> (task_row, old_deadline, new_deadline)
    for row in changes:
        try:
            tid = int(row["task_id"])
        except Exception:
            continue
        t = get_task(tid)
        if not t:
            continue
        if t["status"] not in ("open", "in_progress"):
            continue
        oldd = (row["old_deadline"] or "").strip()
        newd = (row["new_deadline"] or "").strip()
        if oldd == yesterday_str and (newd and newd >= today_str):
            postponed_map[tid] = (t, oldd, newd)

    postponed_yesterday = list(postponed_map.values())
    postponed_ids = set(postponed_map.keys())

    # --- 1-я часть: задачи, которые стали просроченными "за сутки" и НЕ были перенесены
    due_yesterday_rows = get_tasks_due_on(yesterday_str)
    newly_overdue = [
        r for r in due_yesterday_rows
        if r["status"] in ("open", "in_progress") and int(r["id"]) not in postponed_ids
    ]

    # Текст
    lines = []
    lines.append("🧾 <b>Ежедневный отчёт за сутки</b>\n")
    lines.append(f"🌟Задач в работе всего: <b>{total_open_like}</b> (назначенные и взятые в работу)")
    lines.append(f"🔥 Выполнено за сутки: <b>{closed_last_day}</b>")
    lines.append(f"❌ Просроченных задач за сутки: <b>{len(newly_overdue)}</b>")
    lines.append(f"⛔️ Общее число просроченных задач в работе сейчас: <b>{overall_overdue_count}</b>\n")

    if newly_overdue:
        lines.append("<b>Список просроченных задач за сутки:</b>")
        for t in newly_overdue:
            dl = _fmt_date((t["deadline"] or "").strip())
            lines.append(
                f"ID: <code>{t['id']}</code>\n"
                f"Описание: {t['task']}\n"
                f"Исполнитель: {_assignee_with_nick(t['assignee'], t['telegram_id'])}\n"
                f"Дедлайн: {dl}\n"
            )

    if postponed_yesterday:
        lines.append("<b>Список переносов (вчера не успели и перенесли дедлайн):</b>")
        for (t, oldd, newd) in postponed_yesterday:
            lines.append(
                f"ID: <code>{t['id']}</code>\n"
                f"Описание: {t['task']}\n"
                f"Исполнитель: {_assignee_with_nick(t['assignee'], t['telegram_id'])}\n"
                f"Перенос: {_fmt_date(oldd)} → {_fmt_date(newd)}\n"
            )

    lines.append("⬇️Отчёт всех задач по исполнителям внизу⬇️")
    lines.append("Чтобы увидеть список всех просроченных задач введите команду <b>/outdated</b>")
    return "\n".join(lines)

def main():
    now_local = datetime.now(TZINFO)
    # только по будням
    if now_local.weekday() not in WEEKDAYS:
        return

    # 1) текст
    text = build_morning_summary(now_local)

    # 2) персональные PDF по людям (open/in_progress)
    combined_pdf = build_combined_pdf_report(now_local)

    recipients = [str(VADIM_CHAT_ID), *map(str, ASSISTANT_CHAT_IDS)]
    sent = set()
    for cid in recipients:
        cid = (cid or "").strip()
        if not cid or cid in sent:
            continue
        # текст
        try:
            _send(cid, text)
        except Exception:
            pass
        # один общий файл
        if combined_pdf:
            try:
                _send_document(cid, combined_pdf, caption="📎 Отчёт по всем исполнителям")
            
            except Exception:
                pass
        sent.add(cid)

if __name__ == "__main__":
    main()
