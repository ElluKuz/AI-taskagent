# -*- coding: utf-8 -*-
import re
from dateutil import parser as dateparser

TASK_KEYWORDS = [
    "сделай", "нужно", "надо", "проверь", "подготов", "собер", "отправ",
    "исправ", "обнови", "запусти", "рассчитай", "создай", "оформи", "закрой"
]
PRIORITY_WORDS = {"срочно", "важно", "asap", "urgent", "critical"}
MENTION_RE = re.compile(r"@\w+", re.IGNORECASE)

def looks_like_task(text: str) -> bool:
    t = (text or "").lower()
    if len(t.split()) < 3:
        return False
    return any(k in t for k in TASK_KEYWORDS)

def extract_deadline(text: str, default_tz=None) -> str | None:
    try:
        d = dateparser.parse(text, dayfirst=True, fuzzy=True)
        return d.strftime("%Y-%m-%d") if d else None
    except Exception:
        return None

def extract_priority(text: str) -> str:
    t = (text or "").lower()
    return "high" if any(w in t for w in PRIORITY_WORDS) else "normal"

def strip_bot_mention(text: str, bot_username: str) -> str:
    if not bot_username:
        return text
    return re.sub(rf"@{re.escape(bot_username)}\b", "", text or "", flags=re.IGNORECASE).strip()

def detect_assignee(text: str, assignee_names: list[str]) -> tuple[str | None, list[str]]:
    """Эвристика: (однозначное_имя|None, [возможные])"""
    t = (text or "").lower()
    hits = []
    for name in assignee_names:
        nm = (name or "").lower().strip()
        if not nm:
            continue
        if re.search(rf"\b{re.escape(nm)}\b", t):
            hits.append(name)
    if len(hits) == 1:
        return hits[0], hits
    if len(hits) > 1:
        return None, hits
    return None, []
