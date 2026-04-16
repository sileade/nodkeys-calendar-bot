# Nodkeys Calendar & Life Bot v4.1

A powerful, self-hosted Telegram bot that acts as a **unified personal assistant**. Write naturally in Telegram — the bot analyzes your message with **Claude AI** and automatically routes it to the right service: Apple Calendar, Apple Notes, diary, book search, or Kindle delivery.

**No commands needed** — just write what you think.

## How It Works

```
You write in Telegram
        ↓
   Claude AI analyzes
        ↓
┌───────────────────────────────────┐
│  "Meeting tomorrow at 3pm"        │ → 📅 Apple Calendar (event)
│  "Buy groceries"                  │ → ✅ Apple Calendar (task)
│  "Don't forget passport"          │ → 🔔 Apple Calendar (reminder)
│  "Remember: WiFi password 12345"  │ → 📝 Apple Notes (note)
│  "Today I realized I need rest"   │ → 📔 Apple Notes (diary)
│  "Find book Master & Margarita"   │ → 📚 Flibusta → Kindle
│  sent_file.epub                   │ → 📖 Convert → Kindle
│  https://habr.com/article/123     │ → 🔗 Calendar (review task)
└───────────────────────────────────┘
```

## Features

### Intelligent Message Routing

The bot uses **Claude AI** to understand the intent behind every message and routes it to the appropriate service. No slash commands required for core functionality.

| Type | Trigger Examples | Destination |
| --- | --- | --- |
| **Event** | "Meeting with client tomorrow at 3pm" | Apple Calendar |
| **Task** | "Need to finish report by Friday" | Apple Calendar |
| **Reminder** | "Don't forget to call dentist" | Apple Calendar |
| **Note** | "Remember: WiFi password is 12345" | Apple Notes |
| **Diary** | "Today I realized I need more sleep" | Apple Notes (daily diary) |
| **Book Search** | "Find book Master and Margarita" | Flibusta OPDS → Kindle |

### Calendar Management (Apple Calendar via iCloud CalDAV)

| Feature | Description |
| --- | --- |
| **AI Analysis** | Claude AI parses natural language to extract dates, times, and event types |
| **Auto-detect** | Distinguishes between events, tasks, and reminders |
| **URL Detection** | Automatically creates "Review" tasks for shared links |
| **Multi-calendar** | Routes entries to Work, Family, or Reminders calendars |
| **Smart Dates** | Understands "tomorrow", "next Friday", "in a week", etc. |
| **Delete/Cleanup** | Reply "delete" to remove, or use `/delete` command |
| **iCal Proxy** | Built-in proxy server for Homepage calendar widget |

### Apple Notes Integration (via iCloud IMAP)

| Feature | Description |
| --- | --- |
| **Quick Notes** | "Remember this..." creates a note in Apple Notes |
| **Daily Diary** | Personal thoughts are added to a daily diary note with timestamps |
| **Chronography** | Each diary entry includes a timestamp, one note per day |
| **Auto-append** | Multiple diary entries per day are appended to the same note |

### Book Search (Flibusta OPDS)

| Feature | Description |
| --- | --- |
| **Natural Language** | "Find book..." or "I want to read..." triggers search |
| **Flibusta OPDS** | Searches the Flibusta library via OPDS protocol |
| **Format Selection** | Shows available formats (EPUB, FB2, MOBI, etc.) |
| **Kindle Delivery** | Downloads, converts if needed, and sends to Kindle |
| **Interactive** | Inline buttons for book selection |

### Kindle Document Delivery

Send any ebook or document to the bot, and it will analyze the format, convert if necessary, and deliver it to your Kindle device via email.

| Feature | Description |
| --- | --- |
| **AI Format Detection** | OpenAI analyzes the file and recommends the best format |
| **Auto Conversion** | Calibre converts unsupported formats to EPUB |
| **Direct Send** | EPUB, PDF, TXT, DOC, DOCX, RTF, HTML sent without conversion |
| **Metadata Extraction** | Shows book title, author, and language before sending |
| **Multi-device** | Support for multiple Kindle devices with interactive selection |
| **iCloud SMTP** | Sends via iCloud Mail for reliable delivery |

### Supported Ebook Formats

**Direct send (no conversion):** EPUB, PDF, TXT, DOC, DOCX, RTF, HTM, HTML, PNG, JPG, GIF, BMP

**Conversion via Calibre:** FB2, MOBI, AZW, AZW3, AZW4, CBZ, CBR, CB7, CHM, DJVU, LIT, LRF, ODT, PDB, PML, RB, SNB, TCR

## Architecture

```
Telegram Bot (python-telegram-bot)
├── Claude AI Message Router
│   ├── event/task/reminder → Apple Calendar (iCloud CalDAV)
│   ├── note → Apple Notes (iCloud IMAP)
│   ├── diary → Apple Notes Daily Diary (iCloud IMAP)
│   └── book_search → Flibusta OPDS → Kindle
├── Kindle Handler (OpenAI + Calibre + SMTP)
│   ├── AI Format Analysis (OpenAI GPT-4.1-nano)
│   ├── Format Conversion (Calibre ebook-convert)
│   └── Email Delivery (iCloud SMTP)
├── Health Check Server (:8085)
│   ├── /health — bot status and stats
│   ├── /weekly — HTML weekly calendar
│   ├── /books — JSON book history
│   ├── /repos — GitHub repositories
│   └── /download/<file> — download stored books
└── iCal Proxy Server (:8086)
    └── /calendar.ics — combined iCal feed
```

## Quick Start

### Prerequisites

Docker and Docker Compose installed on your server.

### 1. Clone the repository

```shell
git clone https://github.com/sileade/nodkeys-calendar-bot.git
cd nodkeys-calendar-bot
```

### 2. Configure environment

```shell
cp .env.example .env
# Edit .env with your credentials
```

### 3. Build and run

```shell
docker build -t calendar-life-bot:latest .
docker run -d \
  --name calendar-bot \
  --restart unless-stopped \
  --env-file .env \
  -p 8085:8085 \
  -p 8086:8086 \
  calendar-life-bot:latest
```

## Configuration

### Environment Variables

| Variable | Description | Required |
| --- | --- | :-: |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather | **Yes** |
| `TELEGRAM_CHAT_ID` | Allowed Telegram chat ID | **Yes** |
| `CLAUDE_API_KEY` | Anthropic Claude API key | **Yes** |
| `CLAUDE_MODEL` | Claude model name | No |
| `ICLOUD_USERNAME` | iCloud email (used for CalDAV + IMAP Notes) | **Yes** |
| `ICLOUD_PASSWORD` | iCloud app-specific password | **Yes** |
| `CALDAV_URL` | CalDAV server URL | No |
| `TZ` | Timezone (e.g., Europe/Moscow) | No |
| `CALENDAR_FAMILY` | Family calendar name | No |
| `CALENDAR_WORK` | Work calendar name | No |
| `CALENDAR_REMINDERS` | Reminders calendar name | No |
| `KINDLE_EMAIL` | Kindle device email address | **Yes** |
| `KINDLE_EMAIL_FROM` | Sender email (approved in Amazon) | **Yes** |
| `KINDLE_EMAIL_PASSWORD` | Email app-specific password | **Yes** |
| `KINDLE_SMTP_HOST` | SMTP server host | No |
| `KINDLE_SMTP_PORT` | SMTP server port | No |
| `KINDLE_DEVICES` | Multi-device config | No |
| `OPENAI_API_KEY` | OpenAI API key for format analysis | No |
| `FLIBUSTA_BASE_URL` | Flibusta mirror URL | No |

### Multi-Kindle Device Setup

```
KINDLE_DEVICES="My Kindle:email1@kindle.com|Family Kindle:email2@kindle.com"
```

## Bot Commands

| Command | Description |
| --- | --- |
| `/start` | Welcome message and bot info |
| `/help` | Show usage guide with examples |
| `/today` | List today's calendar events |
| `/calendars` | Show configured calendars |
| `/delete <keyword>` | Search and delete events |
| `/cleanup` | Remove all bot-created events |

> **Note:** Core functionality works through natural language — just write what you need. Commands are optional utilities.

## Usage Examples

**Calendar:**
- "Meeting with dentist tomorrow at 10:00" → creates event in Family calendar
- "Finish project report by Friday" → creates task in Work calendar
- "Don't forget to buy milk" → creates reminder

**Notes:**
- "Remember: the WiFi password is SuperSecret123" → saves to Apple Notes
- "Note: interesting article about AI at habr.com/123" → saves to Apple Notes

**Diary:**
- "Today I realized I need to sleep more" → adds to daily diary with timestamp
- "Interesting observation: people are happier on Fridays" → diary entry

**Books:**
- "Find book Master and Margarita" → searches Flibusta, offers to send to Kindle
- "I want to read something by Stephen King" → searches by author
- "Download 1984 Orwell" → finds and sends to Kindle

**Kindle:**
- Send any .epub, .fb2, .pdf file → bot analyzes, converts, sends to Kindle

**Links:**
- Send any URL → automatically creates a "Review" task for today/tomorrow

## API Endpoints

### Health Check

```
GET http://localhost:8085/health
```

Response:

```json
{
  "status": "ok",
  "bot": "Nodkeys Calendar & Life Bot v4.1",
  "uptime_seconds": 3600,
  "messages_processed": 42,
  "kindle_sent": 5,
  "kindle_converted": 3,
  "kindle_errors": 0
}
```

### Weekly Calendar

```
GET http://localhost:8085/weekly
```

Returns HTML with the current week's events.

### Book History

```
GET http://localhost:8085/books
```

Returns JSON with sent books history.

### iCal Proxy

```
GET http://localhost:8086/calendar.ics
```

Returns combined iCal feed from all configured calendars. Useful for Homepage dashboard widget integration.

## Homepage Integration

Add to your Homepage `services.yaml`:

```yaml
- Calendar Bot:
    icon: mdi-robot
    href: https://t.me/your_bot
    description: AI Calendar & Life Bot
    widget:
      type: customapi
      url: http://calendar-bot:8085/health
      mappings:
        - field: status
          label: Status
        - field: uptime_seconds
          label: Uptime
          format: number
        - field: kindle_sent
          label: Kindle Sent
          format: number
```

## Tech Stack

| Component | Technology |
| --- | --- |
| Language | Python 3.12 |
| Bot Framework | python-telegram-bot 22.x |
| AI Router | Anthropic Claude (Sonnet) |
| Format AI | OpenAI GPT-4.1-nano |
| Calendar | iCloud CalDAV |
| Notes & Diary | iCloud IMAP (Apple Notes) |
| Book Search | Flibusta OPDS |
| Conversion | Calibre (ebook-convert) |
| Email | SMTP (iCloud Mail) |
| Container | Docker |

## Changelog

### v4.1 (2026-04-16)
- **Apple Notes integration** — notes saved to Apple Notes via iCloud IMAP
- **Daily diary with chronography** — one note per day, each entry with timestamp
- **Book search via natural language** — "Find book..." triggers Flibusta OPDS search
- **Unified message routing** — all 6 types (event, task, reminder, note, diary, book_search) handled through Claude AI analysis
- **No commands needed** — removed `/book` command, everything works through natural language
- **Kindle integration with book search** — search → select → download → convert → send to Kindle

### v3.2 (2026-04-14)
- Book file storage and download endpoint
- Download button in Kindle Library widget

### v3.0 (2026-04-14)
- Initial release with Calendar + Kindle functionality
- Claude AI for calendar analysis
- OpenAI for format detection
- Calibre conversion pipeline
- iCal proxy for Homepage widget

## License

MIT License. See [LICENSE](LICENSE) for details.
