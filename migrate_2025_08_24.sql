-- migrate_2025_08_24.sql
PRAGMA foreign_keys=off;
BEGIN TRANSACTION;

-- Новые столбцы в tasks (безопасно, если уже есть — будет no-op через IF NOT EXISTS-паттерн ниже)
ALTER TABLE tasks ADD COLUMN priority TEXT DEFAULT 'normal';
ALTER TABLE tasks ADD COLUMN source TEXT DEFAULT 'api';                  -- api|mention|digest
ALTER TABLE tasks ADD COLUMN source_chat_id TEXT;
ALTER TABLE tasks ADD COLUMN source_message_id INTEGER;
ALTER TABLE tasks ADD COLUMN cancel_reason TEXT;

-- Статус уже есть. Допускаем значения: open|in_progress|done|proposed|cancelled

-- Указки по чатам (только последний просмотренный message_id)
CREATE TABLE IF NOT EXISTS chat_offsets (
  chat_id TEXT PRIMARY KEY,
  last_message_id INTEGER,
  updated_at TEXT
);

-- Явная регистрация отслеживаемых чатов (по /track в группе)
CREATE TABLE IF NOT EXISTS tracked_chats (
  chat_id TEXT PRIMARY KEY,
  title TEXT,
  added_at TEXT
);

COMMIT;
PRAGMA foreign_keys=on;
