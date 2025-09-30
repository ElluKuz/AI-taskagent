# -*- coding: utf-8 -*-
import os, tempfile, requests, logging

# берём спец. переменные для Whisper, а если их нет — падаем на OpenAI по умолчанию
WHISPER_BASE_URL = os.environ.get("WHISPER_BASE_URL", "https://api.openai.com/v1").rstrip("/")
WHISPER_API_KEY  = os.environ.get("WHISPER_API_KEY") or os.environ.get("OPENAI_API_KEY")
WHISPER_MODEL    = os.environ.get("WHISPER_MODEL", "whisper-1")

logger = logging.getLogger("bot.voice")

def _openai_transcribe(path: str) -> str:
    url = f"{WHISPER_BASE_URL}/audio/transcriptions"
    headers = {"Authorization": f"Bearer {WHISPER_API_KEY}"} if WHISPER_API_KEY else {}
    files = {
        # Явно укажем MIME — .oga это ogg/opus
        "file": (os.path.basename(path), open(path, "rb"), "audio/ogg"),
        "model": (None, WHISPER_MODEL),
        "response_format": (None, "text"),
    }
    try:
        r = requests.post(url, headers=headers, files=files, timeout=120)
        logger.info("Whisper POST %s -> %s", url, r.status_code)
        # покажем первые 500 символов тела в INFO, чтобы видеть текст/ошибку
        preview = r.text[:500].replace("\n", " ")
        logger.info("Whisper response preview: %s", preview)
        r.raise_for_status()
        return r.text.strip()
    except Exception as e:
        logger.exception("Whisper transcription failed")
        return ""

async def transcribe_telegram_file(bot, file_id: str) -> str:
    """Скачиваем файл от Telegram во временную папку → Whisper → текст. Файл удаляем."""
    tf = tempfile.NamedTemporaryFile(prefix="vt_", delete=False)
    tf.close()
    try:
        f = await bot.get_file(file_id)
        await f.download_to_drive(custom_path=tf.name)
        text = _openai_transcribe(tf.name)
        return text
    finally:
        try:
            os.unlink(tf.name)
        except Exception:
            pass
