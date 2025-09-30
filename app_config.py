# -*- coding: utf-8 -*-
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tasks.db")

# Secrets (no hardcodes here)
MY_SECRET = os.environ.get("MY_SECRET", "")

# Chat adapter / bot token (generic)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# Timezone (default Bali, override via env)
TZ = os.environ.get("TZ", "Asia/Makassar")

# Recipients: comma-separated list in env → list[str]
#   ASSISTANT_CHAT_IDS="123,456,789"
_ASS = os.environ.get("ASSISTANT_CHAT_IDS", "")
ASSISTANT_CHAT_IDS = [x.strip() for x in _ASS.split(",") if x.strip()]

VADIM_CHAT_ID = os.environ.get("VADIM_CHAT_ID", "")

# Schedules / tone
WORK_END_HOUR = int(os.environ.get("WORK_END_HOUR", "18"))
BRAND_VOICE_PREFIX = os.environ.get(
    "BRAND_VOICE_PREFIX",
    "⚠️ Friendly reminder: I’ll ping again if ignored 🙂"
)

# LLM (OpenAI-compatible)
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL  = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL     = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TIMEOUT_S = int(os.environ.get("OPENAI_TIMEOUT_S", "12"))
