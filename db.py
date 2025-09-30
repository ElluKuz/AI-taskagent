# -*- coding: utf-8 -*-
import sqlite3
from contextlib import contextmanager
from datetime import datetime
import os
from app_config import DB_PATH
import json
# ---------- ВСПОМОГАТЕЛЬНО: авто-миграции под новые поля ----------
def _column_names(c, table):
    rows = c.execute(f"PRAGMA table_info({table});").fetchall()
    return {r[1] for r in rows}  # set of column names

def _ensure_schema():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("PRAGMA foreign_keys=OFF;")

        # deadline_changes: by_who
        cols = _column_names(c, "deadline_changes")
        if "by_who" not in cols:
            c.execute("ALTER TABLE deadline_changes ADD COLUMN by_who TEXT;")

        # task_reassignments: by_who
        cols = _column_names(c, "task_reassignments")
        if "by_who" not in cols:
            c.execute("ALTER TABLE task_reassignments ADD COLUMN by_who TEXT;")

        cols = _column_names(c, "tasks")
        if "link" not in cols:
            c.execute("ALTER TABLE tasks ADD COLUMN link TEXT;")

        # NEW: outbox для отложенных сообщений
        c.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          chat_id TEXT NOT NULL,
          text TEXT NOT NULL,
          markup TEXT,                 -- JSON inline-keyboard (может быть NULL)
          not_before TEXT NOT NULL,    -- ISO UTC YYYY-MM-DDTHH:MM:SSZ
          created_at TEXT NOT NULL,
          sent_at TEXT                 -- когда реально отправили (NULL = ещё не отправлено)
        );
        """)
        #создадим индекс, чтобы быстрее выбирать «дозревшие» записи
        c.execute("CREATE INDEX IF NOT EXISTS idx_outbox_not_before ON outbox(not_before)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_outbox_sent_at ON outbox(sent_at)")

        conn.commit()

_ensure_schema()

def assignee_exists_by_tid(telegram_id: str) -> bool:
    with get_conn() as c:
        row = c.execute(
            "SELECT 1 FROM assignees WHERE telegram_id=? LIMIT 1",
            (str(telegram_id),)
        ).fetchone()
        return bool(row)


# -------------------------------------------------------------------

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# ===== Исполнители ============================================================
def add_or_update_assignee(name: str, telegram_id: str, telegram_nickname: str = "", position: str = ""):
    name = (name or "").strip()
    telegram_id = str(telegram_id or "").strip()
    telegram_nickname = (telegram_nickname or "").strip()
    position = (position or "").strip()
    if not name:
        return
    with get_conn() as c:
        if telegram_id:
            c.execute("""
                INSERT INTO assignees(name, telegram_id, telegram_nickname, position)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                  name = excluded.name,
                  telegram_nickname = CASE
                      WHEN excluded.telegram_nickname <> '' THEN excluded.telegram_nickname
                      ELSE assignees.telegram_nickname END,
                  position = CASE
                      WHEN excluded.position <> '' THEN excluded.position
                      ELSE assignees.position END
            """, (name, telegram_id, telegram_nickname, position))
        else:
            cur = c.execute("SELECT id FROM assignees WHERE name = ?", (name,)).fetchone()
            if cur is None:
                # если ID не знаем — не плодим мусор, но можно завести «пустую» запись при желании:
                c.execute(
                    "INSERT INTO assignees(name, telegram_id, telegram_nickname, position) VALUES (?, '', ?, ?)",
                    (name, telegram_nickname, position)
                )
            else:
                c.execute("""
                    UPDATE assignees
                       SET telegram_nickname = CASE WHEN ? <> '' THEN ? ELSE telegram_nickname END,
                           position          = CASE WHEN ? <> '' THEN ? ELSE position END
                     WHERE id = ?
                """, (telegram_nickname, telegram_nickname, position, position, cur["id"]))
def list_unique_assignees():
    with get_conn() as c:
        return c.execute(
            "SELECT DISTINCT name, telegram_id FROM assignees ORDER BY name"
        ).fetchall()

def get_nickname_by_tid(telegram_id: str) -> str:
    with get_conn() as c:
        row = c.execute(
            "SELECT telegram_nickname FROM assignees WHERE telegram_id=? LIMIT 1",
            (str(telegram_id or ""),)
        ).fetchone()
        return (row["telegram_nickname"] or "").strip() if row else ""
# ===== Задачи ================================================================

# ЗАМЕНИТЬ функцию insert_task целиком
# db.py
def insert_task(task, assignee, telegram_id, deadline,
                priority="normal", source="api",
                source_chat_id=None, source_message_id=None,
                status="open", link: str = ""):
    """
    Вставка задачи с поддержкой поля link.
    initial_text_sent заполняем ТОЛЬКО если статус сразу open/in_progress.
    """
    with get_conn() as c:
        c.execute(
            """INSERT INTO tasks(
                   task, assignee, telegram_id, deadline,
                   initial_text_sent, status, created_at,
                   priority, source, source_chat_id, source_message_id, link
               )
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                str(task or "").strip(),
                str(assignee or "").strip(),
                str(telegram_id or "").strip(),
                str(deadline or "").strip(),
                (now_iso() if status in ("open", "in_progress") else ""),
                str(status or "open"),
                now_iso(),
                str(priority or "normal"),
                str(source or "api"),
                str(source_chat_id or ""),
                (int(source_message_id) if source_message_id is not None else None),
                str(link or "")
            ),
        )
        return c.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

def get_tasks_due_on(local_yyyy_mm_dd: str):
    with get_conn() as c:
        return c.execute(
            """
            SELECT * FROM tasks
             WHERE status IN ('open','in_progress')
               AND TRIM(COALESCE(deadline,'')) = ?
             ORDER BY assignee, id
            """,
            (local_yyyy_mm_dd,)
        ).fetchall()

def get_task(task_id):
    with get_conn() as c:
        return c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()

def get_all_tasks():
    with get_conn() as c:
        return c.execute("SELECT * FROM tasks ORDER BY id").fetchall()

def update_task_assignment(task_id, new_assignee, new_telegram_id, by_who: str | None = None):
    with get_conn() as c:
        prev = c.execute("SELECT assignee, telegram_id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not prev: return
        old_assignee, old_tid = prev["assignee"], prev["telegram_id"]
        c.execute("""INSERT INTO task_reassignments
                     (task_id, old_assignee, old_telegram_id, new_assignee, new_telegram_id, at, by_who)
                     VALUES (?,?,?,?,?,?,?)""",
                  (task_id, old_assignee, old_tid, new_assignee, new_telegram_id, now_iso(), by_who or ""))
        c.execute("""UPDATE tasks
                        SET assignee=?, telegram_id=?, updated_at=?
                     WHERE id=?""",
                  (new_assignee, new_telegram_id, now_iso(), task_id))

def set_task_status(task_id, status):
    with get_conn() as c:
        c.execute("UPDATE tasks SET status=?, updated_at=? WHERE id=?", (status, now_iso(), task_id))

def set_task_deadline(task_id, new_deadline, mark_postponed=False, by_who: str | None = None):
    with get_conn() as c:
        cur = c.execute("SELECT deadline FROM tasks WHERE id=?", (task_id,)).fetchone()
        old_deadline = (cur["deadline"] or "") if cur else ""
        if mark_postponed:
            c.execute("""UPDATE tasks
                           SET deadline=?, postponed=1, when_postponed=?, updated_at=?
                         WHERE id=?""",
                      (new_deadline, now_iso(), now_iso(), task_id))
            c.execute("""INSERT INTO deadline_changes (task_id, old_deadline, new_deadline, at, by_who)
                         VALUES (?,?,?,?,?)""",
                      (task_id, old_deadline or "", new_deadline or "", now_iso(), by_who or ""))
        else:
            c.execute("""UPDATE tasks SET deadline=?, updated_at=? WHERE id=?""",
                      (new_deadline, now_iso(), task_id))

def set_task_priority(task_id, priority):
    with get_conn() as c:
        c.execute("UPDATE tasks SET priority=?, updated_at=? WHERE id=?",
                  (priority, now_iso(), task_id))

def set_task_text(task_id, new_text):
    with get_conn() as c:
        c.execute("UPDATE tasks SET task=?, updated_at=? WHERE id=?",
                  (new_text, now_iso(), task_id))

def mark_cancelled(task_id, reason):
    with get_conn() as c:
        c.execute("""UPDATE tasks SET status='cancelled', cancel_reason=?, updated_at=? WHERE id=?""",
                  (reason or "", now_iso(), task_id))

def find_open_tasks_for_user(chat_id):
    uid = str(chat_id)
    with get_conn() as c:
        # найдём все имена для этого chat_id (обычно одно)
        names = c.execute("SELECT DISTINCT name FROM assignees WHERE telegram_id=?", (uid,)).fetchall()
        name_list = [r["name"] for r in names if (r["name"] or "").strip()]
        if not name_list:
            # нет связанного имени — старое поведение
            return c.execute(
                "SELECT * FROM tasks WHERE telegram_id=? AND status IN ('open','in_progress') ORDER BY created_at",
                (uid,)
            ).fetchall()

        # задачи с этим telegram_id ИЛИ (telegram_id пуст и имя совпадает)
        placeholders = ",".join(["?"] * len(name_list))
        params = [uid] + name_list
        sql = f"""
            SELECT * FROM tasks
             WHERE status IN ('open','in_progress') AND (
                   telegram_id = ? OR (
                       (telegram_id IS NULL OR telegram_id = '')
                       AND assignee IN ({placeholders})
                   )
             )
             ORDER BY created_at
        """
        return c.execute(sql, params).fetchall()

def get_overdue_open_tasks(today_local_yyyy_mm_dd: str):
    with get_conn() as c:
        return c.execute(
            """
            SELECT * FROM tasks
             WHERE status IN ('open','in_progress')
               AND deadline IS NOT NULL AND TRIM(deadline) <> ''
               AND deadline < ?
             ORDER BY deadline, assignee, id
            """,
            (today_local_yyyy_mm_dd,)
        ).fetchall()

def count_open_like():
    with get_conn() as c:
        row = c.execute("SELECT COUNT(*) AS n FROM tasks WHERE status IN ('open','in_progress')").fetchone()
        return int(row["n"] or 0)

def count_closed_between(start_iso_utc: str, end_iso_utc: str):
    with get_conn() as c:
        row = c.execute(
            "SELECT COUNT(*) AS n FROM tasks WHERE status='done' AND updated_at BETWEEN ? AND ?",
            (start_iso_utc, end_iso_utc)
        ).fetchone()
        return int(row["n"] or 0)


def tasks_sent_between(start_iso, end_iso):
    with get_conn() as c:
        return c.execute("""SELECT * FROM tasks
                            WHERE initial_text_sent BETWEEN ? AND ?
                              AND status IN ('open','in_progress')
                            ORDER BY assignee, created_at""",
                         (start_iso, end_iso)).fetchall()

def get_reassignments_between(start_iso_utc, end_iso_utc):
    with get_conn() as c:
        return c.execute(
            """SELECT r.*, t.task
               FROM task_reassignments r
               JOIN tasks t ON t.id = r.task_id
               WHERE r.at BETWEEN ? AND ?
               ORDER BY r.at""",
            (start_iso_utc, end_iso_utc)
        ).fetchall()

def get_deadline_changes_between(start_iso_utc, end_iso_utc):
    with get_conn() as c:
        return c.execute(
            """SELECT d.*, t.task, t.assignee, t.telegram_id
               FROM deadline_changes d
               JOIN tasks t ON t.id = d.task_id
               WHERE d.at BETWEEN ? AND ?
               ORDER BY d.at""",
            (start_iso_utc, end_iso_utc)
        ).fetchall()

def get_reassignments_for_task(task_id: int):
    with get_conn() as c:
        return c.execute(
            "SELECT * FROM task_reassignments WHERE task_id=? ORDER BY at", (task_id,)
        ).fetchall()

def get_deadline_changes_for_task(task_id: int):
    with get_conn() as c:
        return c.execute(
            "SELECT * FROM deadline_changes WHERE task_id=? ORDER BY at", (task_id,)
        ).fetchall()

def get_priority(task_id: int) -> str:
    with get_conn() as c:
        r = c.execute("SELECT priority FROM tasks WHERE id=?", (task_id,)).fetchone()
        return (r["priority"] or "normal") if r else "normal"

def fetch_proposed_tasks(limit: int = 30):
    with get_conn() as c:
        return c.execute(
            "SELECT * FROM tasks WHERE status='proposed' ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()

def get_closed_tasks_between(start_iso, end_iso):
    with get_conn() as c:
        return c.execute(
            "SELECT assignee FROM tasks WHERE status='done' AND updated_at BETWEEN ? AND ?",
            (start_iso, end_iso)
        ).fetchall()
        
# ===== Чаты/указки ===========================================================

def get_tasks_by_assignee_openlike(assignee_name: str):
    with get_conn() as c:
        return c.execute(
            "SELECT * FROM tasks WHERE assignee=? AND status IN ('open','in_progress') ORDER BY created_at",
            (assignee_name,)
        ).fetchall()

def get_tasks_by_tid_openlike(telegram_id: str):
    with get_conn() as c:
        return c.execute(
            "SELECT * FROM tasks WHERE telegram_id=? AND status IN ('open','in_progress') ORDER BY created_at",
            (str(telegram_id),)
        ).fetchall()

def _wipe_open_like():
    with get_conn() as c:
        c.execute("""DELETE FROM deadline_changes
                     WHERE task_id IN (SELECT id FROM tasks WHERE status IN ('proposed','open','in_progress'))""")
        c.execute("""DELETE FROM task_reassignments
                     WHERE task_id IN (SELECT id FROM tasks WHERE status IN ('proposed','open','in_progress'))""")
        cur = c.execute("SELECT COUNT(*) AS n FROM tasks WHERE status IN ('proposed','open','in_progress')").fetchone()
        n = cur["n"] if cur else 0
        c.execute("DELETE FROM tasks WHERE status IN ('proposed','open','in_progress')")
        c.execute("DELETE FROM user_states")
        return n

def track_chat(chat_id: str, title: str):
    with get_conn() as c:
        c.execute("""INSERT INTO tracked_chats(chat_id, title, added_at)
                     VALUES (?,?,?)
                     ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title""",
                  (str(chat_id), title or "", now_iso()))

def list_tracked_chats():
    with get_conn() as c:
        return c.execute("SELECT chat_id, title FROM tracked_chats").fetchall()

def get_last_chat_offset(chat_id: str) -> int:
    with get_conn() as c:
        row = c.execute("SELECT last_message_id FROM chat_offsets WHERE chat_id=?",
                        (str(chat_id),)).fetchone()
    return int(row["last_message_id"]) if (row and row["last_message_id"] is not None) else 0

def set_last_chat_offset(chat_id: str, message_id: int):
    with get_conn() as c:
        c.execute("""INSERT INTO chat_offsets(chat_id, last_message_id, updated_at)
                     VALUES (?,?,?)
                     ON CONFLICT(chat_id) DO UPDATE SET last_message_id=excluded.last_message_id,
                                                     updated_at=excluded.updated_at""",
                  (str(chat_id), int(message_id), now_iso()))
# ===== Таймер ===========================================================
def enqueue_outbox(chat_id: str, text: str, markup: dict | None, not_before_iso_utc: str):
    with get_conn() as c:
        c.execute(
            "INSERT INTO outbox(chat_id, text, markup, not_before, created_at) VALUES (?,?,?,?,?)",
            (str(chat_id), text, (json.dumps(markup) if markup else None), not_before_iso_utc, now_iso())
        )

def pop_due_outbox(now_iso_utc: str, limit: int = 100):
    with get_conn() as c:
        return c.execute(
            """SELECT * FROM outbox
               WHERE sent_at IS NULL AND not_before <= ?
               ORDER BY id LIMIT ?""",
            (now_iso_utc, limit)
        ).fetchall()

def mark_outbox_sent(outbox_id: int):
    with get_conn() as c:
        c.execute("UPDATE outbox SET sent_at=? WHERE id=?", (now_iso(), outbox_id))