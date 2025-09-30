-- Создать файл БД:
--   sqlite3 /home/loyo/projects/VadimsTasks/tasks.db < /home/loyo/projects/VadimsTasks/create_db.sql

PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task TEXT NOT NULL,
  assignee TEXT NOT NULL,
  telegram_id TEXT NOT NULL,
  deadline TEXT,                              -- ISO YYYY-MM-DD или пусто
  initial_text_sent TEXT,                     -- ISO DATETIME, когда отправили Initial последнему исполнителю
  postponed INTEGER DEFAULT 0,                -- 0/1
  when_postponed TEXT,                        -- ISO DATETIME последнего переноса
  status TEXT DEFAULT 'open',                 -- open|in_progress|done
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT
);

-- маленькое хранилище состояний диалогов (ждём дату и т.п.)
CREATE TABLE IF NOT EXISTS user_states (
  user_id TEXT PRIMARY KEY,
  state TEXT,           -- e.g. 'awaiting_new_deadline_for_task:<task_id>'
  payload TEXT,         -- json или текст с id задачи
  updated_at TEXT
);

-- Таблица справочник исполнителей (имя → chat_id) для меню переназначений
CREATE TABLE IF NOT EXISTS assignees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  telegram_id TEXT NOT NULL
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee);
CREATE INDEX IF NOT EXISTS idx_tasks_telegram ON tasks(telegram_id);
CREATE INDEX IF NOT EXISTS idx_tasks_deadline ON tasks(deadline);
CREATE INDEX IF NOT EXISTS idx_tasks_initial ON tasks(initial_text_sent);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

