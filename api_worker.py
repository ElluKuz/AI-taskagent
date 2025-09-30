# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
import logging, os, requests
from dateutil import parser as dateparser
from app_config import MY_SECRET, BOT_TOKEN, VADIM_CHAT_ID, ASSISTANT_CHAT_IDS
from db import insert_task, add_or_update_assignee

LOG_FILE = os.path.join(os.path.dirname(__file__), "api.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("api_worker")

app = Flask(__name__)
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from db import enqueue_outbox
from app_config import TZ

TZINFO = ZoneInfo(TZ)
WEEKDAYS = {0,1,2,3,4}
GRACE_MINUTES = 30

def _in_task_alert_window(dt_local):
    local = dt_local.astimezone(TZINFO)
    if local.weekday() not in WEEKDAYS:
        return False
    if local.hour < 9:
        return False
    if local.hour > 18 or (local.hour == 18 and local.minute > GRACE_MINUTES):
        return False
    return True

def _next_work_morning(dt_local):
    d = dt_local.astimezone(TZINFO)
    if d.hour > 18 or (d.hour == 18 and d.minute > GRACE_MINUTES):
        d = d + timedelta(days=1)
    d = d.replace(hour=9, minute=0, second=0, microsecond=0)
    while d.weekday() not in WEEKDAYS:
        d = (d + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

import re
from datetime import date as _date

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def norm_deadline(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    try:
        if ISO_DATE_RE.match(s):
            y, m, d = map(int, s.split("-"))
            _ = _date(y, m, d)  # валидация
            cand = s
        else:
            dtt = dateparser.parse(s, dayfirst=True, fuzzy=True)
            if not dtt:
                return ""
            cand = dtt.strftime("%Y-%m-%d")

        today = datetime.now(ZoneInfo(TZ)).date()
        y, m, d = map(int, cand.split("-"))
        if _date(y, m, d) < today:
            return ""  # в прошлом — не принимаем
        return cand
    except Exception:
        return ""


def notify_assistant_proposed(task_id: int, task_text: str, assignee: str, deadline: str, priority: str):
    pr = "Важная 🔥" if (priority or "normal") == "high" else "Обычная"
    txt = (
        "Обнаружена задача — нужно подтвердить\n\n"
        f"🧩 Описание: {task_text}\n"
        f"🤡 Исполнитель: {assignee or '—'}\n"
        f"📅 Дедлайн: {deadline or '—'}\n"
        f"❗️ Приоритет: {pr}\n\n"
        f"ID: #{task_id}\n\n"
        "Введите команду /checktasks, чтобы подтвердить и отправить в работу"
    )
    now = datetime.now(TZINFO)
    if _in_task_alert_window(now):
        # слать сразу
        for chat_id in ASSISTANT_CHAT_IDS:
            try:
                r = requests.post(
                    f"{TG_API}/sendMessage",
                    json={"chat_id": str(chat_id), "text": txt},
                    timeout=15,
                )
                if not r.ok:
                    log.error("notify_assistant_proposed -> %s for %s: %s", r.status_code, chat_id, r.text[:300])
            except Exception as e:
                log.error("notify_assistant_proposed failed for %s: %s", chat_id, e)
    else:
        # положить в outbox до 09:00 ближайшего рабочего дня
        not_before = _next_work_morning(now)
        for chat_id in ASSISTANT_CHAT_IDS:
            try:
                enqueue_outbox(str(chat_id), txt, None, not_before)
            except Exception as e:
                log.error("enqueue_outbox failed for %s: %s", chat_id, e)



@app.post("/zap/new_task")
def zap_new_task():
    payload = request.get_json(silent=True) or {}
    log.info(f"ZAP payload: {payload}")
    if payload.get("SECRET_KEY") != MY_SECRET:
        return jsonify({"error": "unauthorized"}), 401

    task = (payload.get("task") or "").strip()
    assignee = (payload.get("assignee") or "").strip()
    telegram_id = str(payload.get("telegram_id") or "").strip()
    deadline = norm_deadline((payload.get("deadline") or "").strip())
    priority = (payload.get("priority") or "normal").lower()

    if not task:
        return jsonify({"error": "missing task"}), 400

    # исполителя можем не знать на этапе ZAP — ок, помощник поправит
    if assignee and telegram_id:
        add_or_update_assignee(assignee, telegram_id)

    # КЛЮЧЕВОЕ: создаём "proposed" — всегда через помощника
    task_id = insert_task(
        task, assignee or "", telegram_id or "", deadline,
        priority=priority, source="api", status="proposed"
    )

    notify_assistant_proposed(task_id, task, assignee, deadline, priority)
    return jsonify({"status": "ok", "task_id": task_id})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005)
