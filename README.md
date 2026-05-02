# Nodkeys Calendar & Life Bot v8.2

A personal AI-powered Telegram bot that serves as a unified life management hub. It uses **Claude AI with native Tool Calling** to understand natural language and automatically route requests to the right system: Apple Calendar, Apple Notes, book search, server management, expense tracking, and more.

No commands needed — just write or speak naturally.

## Architecture

The bot is built around Claude's **Tool Calling** architecture. Instead of parsing JSON responses, Claude directly invokes typed functions (tools) based on the user's intent. This makes the system highly extensible — adding a new capability requires only defining a new tool schema and its handler.

| Component | Technology |
|---|---|
| AI Engine | Claude (Anthropic) with native Tool Calling |
| Calendar | Apple iCloud CalDAV |
| Notes | Apple Notes via IMAP |
| Book Search | Flibusta OPDS + Anna's Archive + Jackett |
| E-book Delivery | Calibre conversion + SMTP to Kindle |
| Server Management | Docker CLI via socket mount |
| Web Search | DuckDuckGo HTML parser |
| Data Storage | JSON files (persistent Docker volume) |
| Containerization | Docker with health checks |

## Features

### Calendar & Task Management

The bot analyzes any text message and determines whether it contains a task, event, or reminder. It automatically extracts the date, time, duration, and category, then creates the appropriate entry in Apple Calendar.

| Capability | How it works |
|---|---|
| Create events | "Встреча с врачом завтра в 15:00" → creates calendar event |
| Create tasks | "Купить молоко" → creates task with deadline |
| Create reminders | "Напомни позвонить маме в 18:00" → reminder with notification |
| Edit events | "Перенеси встречу на пятницу" → finds and reschedules |
| Delete events | Reply to bot's confirmation with "удали" |
| Recurring tasks | "Каждый понедельник планёрка в 10:00" → recurring event |
| Category routing | Auto-categorizes into Работа/Дом/Личное/Долгосрочные |
| Multi-calendar | Routes to Family or Work calendar based on user rules |

### Proactive Intelligence

The bot does not just respond — it actively monitors your schedule and provides insights.

| Feature | Description |
|---|---|
| Morning briefing | Daily at 08:00: today's events, weather, overdue tasks, habits |
| Weekly review | Sunday at 20:00: week summary, habit stats, upcoming week |
| Day overload warning | Alerts when a day has 5+ events or 6+ hours booked |
| Free slot finder | "Найди окно на 2 часа" → suggests available time slots |
| Weather integration | Current weather included in morning briefing |
| Financial triggers | Reminders on 1st, 10th, 25th for rent, utilities, salary |
| Pattern analysis | Monday insights: productivity patterns, neglected contacts |
| Meeting preparation | Auto-generates briefing 30-120 min before important meetings |
| Workflow triggers | "Командировка" → auto-suggests packing list, taxi, etc. |

### Habit Tracking

A full habit tracking system with streaks, statistics, and natural language interaction.

| Action | Example |
|---|---|
| Add habit | "Хочу отслеживать медитацию" |
| Mark done | "Сделал зарядку" |
| View stats | "Как мои привычки?" |
| Auto-create | Marking an unknown habit auto-creates it |

### Notes & Diary

| Feature | Description |
|---|---|
| Quick notes | "Запиши: идея для проекта..." → Apple Notes |
| Daily diary | "Дневник: сегодня был продуктивный день" → chronological diary |
| Memory system | "Запомни, что у жены аллергия на орехи" → long-term memory |

### Book Management

| Feature | Description |
|---|---|
| Book search | "Найди книгу Мастер и Маргарита" → Flibusta + Anna's Archive |
| Format selection | Choose fb2/epub/mobi/pdf from search results |
| Kindle delivery | Auto-converts to Kindle format and sends via email |
| X-Ray analysis | AI-powered book analysis: characters, themes, timeline |
| Clippings import | Parse Kindle My Clippings.txt → key takeaways |
| URL to Kindle | Send any URL → clean EPUB → Kindle |

### Content & Media

| Feature | Description |
|---|---|
| YouTube summary | Send YouTube link → transcription → AI summary |
| Read-It-Later | All URLs auto-saved; weekend briefing reminds about them |
| Photo recognition | Send photo of receipt/bill → creates payment task |
| Voice messages | Voice → transcription → AI analysis → action |
| Geo-reminders | Send location → checks nearby task reminders |

### Server Management

The bot has direct access to the Docker host and can manage infrastructure.

| Feature | Description |
|---|---|
| Container status | "Статус серверов" → shows all containers with health |
| Container management | "Перезапусти radarr" → restarts the container |
| Server commands | "Покажи место на диске" → executes df -h |
| Safety guards | Dangerous commands (rm -rf /, etc.) are blocked |

### Financial Tracking

| Feature | Description |
|---|---|
| Record expenses | "Потратил 1500 на такси" → saves to expense tracker |
| Receipt recognition | Photo of receipt → extracts amount, category, description |
| Expense reports | "Сколько потратил за месяц?" → breakdown by category |
| Visual analytics | Bar charts showing spending distribution |

### Knowledge Base

The bot automatically learns from every interaction and builds a searchable knowledge base.

| Feature | Description |
|---|---|
| Auto-learning | Every request/response pair is saved |
| Context awareness | Recent interactions are included in Claude's context |
| Search | "Что я спрашивал про сервера?" → searches history |
| Categories | Entries tagged: general, server, finance, tasks |

### Web Search

| Feature | Description |
|---|---|
| General search | "Погода в Москве" → DuckDuckGo search results |
| Multi-step planning | "Спланируй поездку в Стамбул" → step-by-step plan |

## Environment Variables

| Variable | Description | Required |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API token | Yes |
| `TELEGRAM_CHAT_ID` | Primary allowed chat ID | Yes |
| `CLAUDE_API_KEY` | Anthropic API key | Yes |
| `CLAUDE_MODEL` | Claude model name | No (default: claude-sonnet-4-6) |
| `ICLOUD_USERNAME` | iCloud email for CalDAV/IMAP | Yes |
| `ICLOUD_PASSWORD` | iCloud app-specific password | Yes |
| `CALDAV_URL` | CalDAV server URL | No (default: https://caldav.icloud.com/) |
| `CALENDAR_FAMILY` | Family calendar name | No |
| `CALENDAR_WORK` | Work calendar name | No |
| `CALENDAR_REMINDERS` | Reminders calendar name | No |
| `TELEGRAM_PROXY_URL` | SOCKS5 proxy for Telegram | No |
| `CLAUDE_PROXY_URL` | SOCKS5 proxy for Claude API | No |
| `TELEGRAM_BASE_URL` | Custom Telegram API base URL | No |
| `ALLOWED_CHAT_IDS` | Comma-separated allowed chat IDs | No |
| `GROUP_USERS` | Per-user routing rules | No |
| `TZ` | Timezone | No (default: Europe/Moscow) |
| `FLIBUSTA_BASE_URL` | Flibusta mirror URL | No |
| `FLIBUSTA_PROXY_URL` | SOCKS5 proxy for Flibusta | No |

## Docker Compose

```yaml
calendar-bot:
  build: ./calendar-bot
  container_name: calendar-bot
  restart: unless-stopped
  environment:
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
    - CLAUDE_API_KEY=${CLAUDE_API_KEY}
    - ICLOUD_USERNAME=${ICLOUD_USERNAME}
    - ICLOUD_PASSWORD=${ICLOUD_PASSWORD}
    - TZ=Europe/Moscow
  volumes:
    - ./calendar-bot/data:/app/data
    - /var/run/docker.sock:/var/run/docker.sock
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8085/health"]
    interval: 30s
    timeout: 10s
    retries: 3
```

## Data Persistence

All data is stored in JSON files under `/app/data/` (mounted as a Docker volume):

| File | Purpose |
|---|---|
| `reminders.json` | Scheduled reminders |
| `habits.json` | Habit definitions and check-ins |
| `memory.json` | Long-term memory facts |
| `knowledge_base.json` | Auto-learning interaction history |
| `expenses.json` | Financial records |
| `read_later.json` | Saved URLs for later reading |
| `event_store.json` | Event ID mapping for edit/delete |
| `workflows.json` | Contextual workflow triggers |
| `patterns.json` | Productivity pattern data |
| `financial_triggers.json` | Financial reminder config |
| `geo_reminders.json` | Location-based reminders |

## Version History

| Version | Key Changes |
|---|---|
| v4.2 | Initial release: calendar, notes, book search |
| v5.0 | Multi-chat support, per-user routing, Kindle integration |
| v5.1 | Category auto-detection for reminders |
| v5.2 | Voice messages, morning briefing, habit tracking |
| v5.3 | AI-driven natural language for habits and views |
| v6.0 | Proactive features: weather, patterns, meeting prep |
| v7.0 | Tool Calling architecture (Claude native functions) |
| v8.0 | Knowledge base, expense tracker, server management, web search |
| v8.1 | Fix Tool Calling routing (removed legacy JSON confidence/action checks) |
