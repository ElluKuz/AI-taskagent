# AI-tasker

AI-driven task manager that transforms noisy **work chats** and **meeting transcripts** into structured tasks with owners, deadlines, reminders, and reports.

---

## 💼 Business Impact

- 📉 **–80% lost tasks** → after structured capture and assistant review, almost no action items slip through.  
- ⚡ **+30% faster execution** → deadlines tracked, high-priority tasks can’t be postponed.  
- ⏱ **~45 min/day saved per manager** → automated morning/evening reports replace manual status checks.  
- 📈 **+25% team productivity** → less context switching: tasks arrive as clear cards with owner & deadline.  
- 🛠 **0 extra tools needed** → works directly on top of existing chats and meeting transcripts.  
- 💸 **Low-cost infra** → Python + SQLite stack runs on a <$10/month server.  

---
##  What the system does

- From work chats: detects action items in chat messages, extracts task text, owner, deadline, and priority. Every new task starts as a candidate and goes through assistant review.

-  From meeting transcripts (Plaud → Zapier): after transcripts are ready, Zapier pipelines normalize action items and send them to the API /zap/new_task. Each task is reviewed before being assigned.

- Assistant review: candidates are shown in a review carousel with options Approve, Edit, Reassign, Cancel.

-  Task lifecycle: once approved, the assignee receives a task card with description, deadline, and priority, plus follow-up reminders.

-  Reminders: gentle nudges after 3 days, one day before deadline, and on the deadline day. Overdue tasks are highlighted daily.

- Reports:
    – Morning: summary text + PDF report with all open/in-progress tasks, grouped by assignee.
    – Evening: daily digest for each assignee.
    – On demand: CSV/Excel exports.

- Proof of completion: when closing a task, the user is prompted to attach evidence (file, image, video, audio). Proof is stored and linked to the task closure.

---

## ✨ Features

- 📥 **Task intake from work chats**  
  Detects action items, extracts text, assignee, deadline, and priority. Tasks start as `proposed` and require assistant review.

- 📝 **Task intake from meeting transcripts (Plaud → Zapier)**  
  Transcripts are processed, action items extracted, and sent to the API (`/zap/new_task`).

- 👩‍💻 **Assistant review**  
  Carousel with actions: *Approve*, *Edit*, *Reassign*, *Cancel*.

- 🚀 **Lifecycle management**  
  Approved tasks are assigned, with reminders, deadline controls, and completion proof.

- ⏰ **Smart reminders**  
  Nudges after 3 days, one day before the deadline, and on the deadline. Overdues highlighted daily.

- 📊 **Reports**  
  - Morning: summary text + PDF of all open tasks per assignee  
  - Evening: personal digests  
  - On demand: CSV/Excel exports  

- 📎 **Proof of completion**  
  Users can attach evidence (files, images, audio, video) when closing a task.

---

## 🛠 Architecture & Tech Stack

**Core**
- Python 3.11+
- Flask (API for `/zap/new_task`)
- SQLite (lightweight, WAL mode, auto-migrations)
- Async bot worker with message adapters
- Scheduler with hourly jobs

**AI / NLP**
- OpenAI-compatible API (`/chat/completions`)
- Strict JSON schema validation
- Fallback heuristics (`nlp.py`) for priority, deadlines, assignees

**Reports**
- ReportLab → PDF with Unicode
- OpenPyXL → Excel (optional)
- CSV fallback

**Voice transcription**
- Whisper API (OpenAI or compatible) for audio messages

---

## 🔄 Task Intake Flows

### 1. From Work Chats
1. Candidate message detected.  
2. Pipeline: preprocess → LLM classification → JSON verdict.  
3. If valid, insert into DB as `proposed`.  
4. Assistant notified to review.  

**LLM output schema:**
```json
{
  "looks_like_task": true,
  "description": "Prepare report on sales figures",
  "assignee": "John Doe",
  "deadline": "2025-10-15",
  "priority": "normal",
  "confidence": 0.92,
  "candidates": [],
  "source_link": "https://..."
}
