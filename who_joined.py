# -*- coding: utf-8 -*-
import os, sqlite3, asyncio
from telegram import Bot
from telegram.error import Forbidden, BadRequest

BOT_TOKEN = os.environ.get("BOT_TOKEN") or "7950588604:AAFyKm_ejwUBXB7tKMtBHVmM6C8OQsaOOEg"
DB_PATH = "/home/loyo/projects/VadimsTasks/tasks.db"


async def can_dm(bot: Bot, tid: str) -> bool:
    try:
        # достаточно getChat или send_chat_action
        await bot.get_chat(int(tid))
        return True
    except (Forbidden, BadRequest):
        return False
    except Exception:
        return False

async def main():
    bot = Bot(token=BOT_TOKEN)
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    c = conn.cursor()
    rows = c.execute("""
        SELECT DISTINCT name, telegram_id
        FROM assignees
        WHERE telegram_id IS NOT NULL AND telegram_id <> ''
        ORDER BY name
    """).fetchall()
    ok, not_ok = [], []
    for r in rows:
        tid = str(r["telegram_id"])
        if await can_dm(bot, tid):
            ok.append((r["name"], tid))
        else:
            not_ok.append((r["name"], tid))
    print("✅ МОГУ ПИСАТЬ:")
    for n, t in ok: print(f"  {n} [{t}]")
    print("\n❌ НЕ МОГУ ПИСАТЬ (не нажал /start или неверный id):")
    for n, t in not_ok: print(f"  {n} [{t}]")
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
