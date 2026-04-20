# Nodkeys Calendar & Life Bot v5.0

A powerful, self-hosted Telegram bot that acts as a **unified personal assistant**. Write naturally in Telegram — the bot analyzes your message with **Claude AI** and automatically routes it to the right service: Apple Calendar, Apple Notes, diary, book search, X-Ray analysis, URL-to-Kindle, or Kindle delivery.

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
│  "X-Ray по Войне и миру"         │ → 🔬 AI literary analysis
│  "На киндл https://habr.com/..."  │ → 🌐 URL → EPUB → Kindle
│  sent_file.epub                   │ → 📖 Convert → Kindle
│  My Clippings.txt                 │ → 📎 AI analysis of highlights
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
| **Book Search** | "Find book Master and Margarita" | Flibusta/Anna's Archive/Jackett → Kindle |
| **X-Ray** | "X-Ray по Мастеру и Маргарите" | AI literary analysis |
| **URL → Kindle** | "На киндл https://habr.com/article" | Download → EPUB → Kindle |

### Calendar Management (Apple Calendar via iCloud CalDAV)

| Feature | Description |
| --- | --- |
| **AI Analysis** | Claude AI parses natural language to extract dates, times, and event types |
| **Auto-detect** | Distinguishes between events, tasks, and reminders |
| **URL Detection** | Automatically creates "Review" tasks for shared links |
| **Multi-calendar** | Routes entries to Work, Family, or Reminders calendars |
| **Per-User Routing** | In group chats, each user can have their own calendar routing rule |
| **Smart Dates** | Understands "tomorrow", "next Friday", "in a week", etc. |
| **Delete/Cleanup** | Reply "delete" to remove, or use `/delete` command |
| **iCal Proxy** | Built-in proxy server for Homepage calendar widget |

### Group Chat & Per-User Calendar Routing (NEW in v5.1)

The bot supports **group chats** with per-user calendar routing. Each group member can have their own routing rule:

| Rule | Behavior | Example |
| --- | --- | --- |
| `family` | All calendar entries forced to Family calendar | Vera → always Family |
| `work` | All calendar entries forced to Work calendar | — |
| `auto` | Claude AI decides based on content (default) | @seleadi → Work or Family |

Configure via `GROUP_USERS` environment variable:

```
GROUP_USERS=vera_username:Вера:family|seleadi:Ilea:auto
```

User matching supports: Telegram username, user ID, or first name (case-insensitive). The bot also sends sender context to Claude for better routing decisions in group chats.

### Apple Notes Integration (via iCloud IMAP)

| Feature | Description |
| --- | --- |
| **Quick Notes** | "Remember this..." creates a note in Apple Notes |
| **Daily Diary** | Personal thoughts are added to a daily diary note with timestamps |
| **Chronography** | Each diary entry includes a timestamp, one note per day |
| **Auto-append** | Multiple diary entries per day are appended to the same note |

### Book Search (Flibusta, Anna's Archive, Jackett)

| Feature | Description |
| --- | --- |
| **Natural Language** | "Find book..." or "I want to read..." triggers search |
| **Multi-Source Search** | Searches Flibusta (OPDS + HTML fallback), Anna's Archive, and Jackett |
| **AI Rethink** | If a book isn't found, Claude AI suggests alternative titles and retries |
| **Smart Ranking** | Results are ranked by relevance (exact match, author match, language) |
| **Format Selection** | Shows available formats (EPUB, FB2, MOBI, etc.) |
| **Kindle Delivery** | Downloads, converts if needed, and sends to Kindle |
| **Interactive** | Inline buttons for book selection and Kindle device selection |

### X-Ray Book Analysis (NEW in v5.0)

Generate a structured literary analysis for any book, inspired by Amazon Kindle X-Ray.

| Feature | Description |
| --- | --- |
| **Characters** | Up to 10 key characters with roles and descriptions |
| **Themes** | Main themes and motifs of the book |
| **Locations** | Key locations and their significance |
| **Timeline** | Chronological overview of events |
| **Fun Facts** | Interesting facts about the book and author |
| **Spoiler-Free** | Specify reading progress (%) to avoid spoilers |

Usage: `/xray Мастер и Маргарита` or write "сделай x-ray по Войне и миру"

### URL → Kindle (NEW in v5.0)

Send any web article to your Kindle as a clean EPUB.

| Feature | Description |
| --- | --- |
| **Smart Extraction** | Extracts main article content, removes ads and navigation |
| **Clean EPUB** | Converts to well-formatted EPUB with proper typography |
| **Calibre Fallback** | Uses Calibre for conversion, falls back to HTML if unavailable |
| **Device Selection** | Choose which Kindle device to send to |
| **History Tracking** | All sent articles are saved in book history |

Usage: "Отправь на киндл https://habr.com/article/123" or "На читалку https://medium.com/post"

### Kindle Clippings Analysis (NEW in v5.0)

Send your Kindle's `My Clippings.txt` file to get an AI-powered analysis of your highlights.

| Feature | Description |
| --- | --- |
| **Auto-Detection** | Automatically detects Clippings files by name |
| **Multi-Book Parsing** | Groups highlights by book, removes duplicates |
| **AI Summary** | Claude generates Key Takeaways and Best Quotes per book |
| **Action Items** | Practical suggestions based on your reading highlights |
| **Reading Patterns** | Overall themes and patterns across your reading |

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
├── Multi-Chat Authorization (ALLOWED_CHAT_IDS + GROUP_USERS)
├── Per-User Calendar Routing (family/work/auto per user)
├── Claude AI Message Router
│   ├── event/task/reminder → Apple Calendar (iCloud CalDAV)
│   ├── note → Apple Notes (iCloud IMAP)
│   ├── diary → Apple Notes Daily Diary (iCloud IMAP)
│   ├── book_search → Flibusta OPDS → Kindle
│   ├── xray → Claude AI Literary Analysis
│   └── url_to_kindle → Download → EPUB → Kindle
├── Kindle Handler (OpenAI + Calibre + SMTP)
│   ├── AI Format Analysis (OpenAI GPT-4.1-nano)
│   ├── Kindle Clippings Parser + AI Summary
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
docker build -t calendar-bot:v5.0 .
docker run -d \
  --name calendar-bot \
  --restart unless-stopped \
  --env-file .env \
  -v ./data:/app/data \
  -p 8085:8085 \
  -p 8086:8086 \
  calendar-bot:v5.0
```

## Configuration

### Environment Variables

| Variable | Description | Required |
| --- | --- | :-: |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather | **Yes** |
| `TELEGRAM_CHAT_ID` | Primary allowed Telegram chat ID | **Yes** |
| `ALLOWED_CHAT_IDS` | Additional allowed chat IDs (comma-separated) | No |
| `GROUP_USERS` | Per-user routing rules for group chats | No |
| `TELEGRAM_PROXY_URL` | SOCKS5 proxy for Telegram API | No |
| `TELEGRAM_BASE_URL` | Custom Telegram API base URL | No |
| `CLAUDE_API_KEY` | Anthropic Claude API key | **Yes** |
| `CLAUDE_MODEL` | Claude model name | No |
| `CLAUDE_PROXY_URL` | SOCKS5 proxy for Claude API | No |
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
| `KINDLE_TMP_DIR` | Temp directory for file processing | No |
| `KINDLE_BOOKS_STORAGE` | Persistent book storage path | No |
| `KINDLE_BOOKS_DB` | Book history JSON database path | No |
| `GITHUB_TOKEN` | GitHub PAT for `/repos` endpoint | No |
| `OPENAI_API_KEY` | OpenAI API key for format analysis | No |
| `FLIBUSTA_BASE_URL` | Flibusta mirror URL | No |
| `FLIBUSTA_PROXY_URL` | SOCKS5 proxy for Flibusta | No |

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
| `/book <query>` | Search for books on Flibusta |
| `/xray <title>` | Generate X-Ray analysis for a book |
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

**X-Ray (NEW):**
- `/xray Мастер и Маргарита` → characters, themes, locations, timeline
- "Сделай x-ray по Войне и миру" → AI literary analysis
- "X-ray 1984 Orwell, я на 50%" → spoiler-free analysis up to 50%

**URL → Kindle (NEW):**
- "Отправь на киндл https://habr.com/article/123" → downloads, cleans, sends as EPUB
- "На читалку https://medium.com/post" → article to Kindle

**Kindle Clippings (NEW):**
- Send `My Clippings.txt` file → AI-powered analysis of your highlights

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
  "bot": "Nodkeys Calendar & Life Bot v5.0",
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
    description: AI Calendar & Life Bot v5.0
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

## Deploy & Rollback

See [DEPLOY.md](DEPLOY.md) for detailed deployment instructions, rollback procedures, and monitoring checklist.

## Tech Stack

| Component | Technology |
| --- | --- |
| Language | Python 3.12 |
| Bot Framework | python-telegram-bot 22.x |
| AI Router | Anthropic Claude (Sonnet) |
| Format AI | OpenAI GPT-4.1-nano |
| Calendar | iCloud CalDAV |
| Notes & Diary | iCloud IMAP (Apple Notes) |
| Book Search | Flibusta OPDS + HTML, Anna's Archive, Jackett |
| Article Extraction | BeautifulSoup4 |
| Conversion | Calibre (ebook-convert) |
| Email | SMTP (iCloud Mail) |
| Container | Docker |

## Changelog

### v5.1 (2026-04-20)
- **Per-User Calendar Routing** — group chat support with per-user routing rules: each member can have their calendar entries forced to a specific calendar (family/work) or let Claude decide (auto)
- **Multi-Chat Authorization** — `ALLOWED_CHAT_IDS` supports multiple chat IDs (personal + group chats); `GROUP_USERS` configures per-user routing with username/ID/name matching
- **Sender Context for Claude** — Claude receives sender identity in group chats for better calendar routing decisions
- **URL Task Routing** — auto-detected URL tasks now respect per-user calendar rules
- **36 Unit Tests** — expanded test suite with routing tests (username match, case-insensitive, ID match, override logic)

### v5.0 (2026-04-17)
- **X-Ray Book Analysis** — `/xray` command and natural language trigger for AI-powered literary analysis (characters, themes, locations, timeline, fun facts) with spoiler-free mode
- **URL → Kindle** — send web articles to Kindle as clean EPUB; smart content extraction removes ads, navigation, and trackers; Calibre conversion with HTML fallback
- **Kindle Clippings Parser** — auto-detects `My Clippings.txt`, parses highlights by book, generates AI summary with Key Takeaways and Action Items
- **18 Bug Fixes** — URL regex trailing punctuation, unused imports, f-string fixes, ical_proxy rewrite with proper RFC 5545 compliance, path traversal sanitization
- **Dockerfile Improvements** — added HEALTHCHECK, VOLUME for persistent data, EXPOSE ports
- **Complete .env.example** — documented all environment variables including proxies and storage paths
- **Deploy & Rollback Guide** — added DEPLOY.md with step-by-step deploy, rollback, and monitoring checklist

### v4.2 (2026-04-17)
- **Enhanced Book Search** — added HTML web-parsing fallback for Flibusta, plus Anna's Archive and Jackett integrations
- **AI Rethink for Books** — Claude AI automatically suggests alternative titles if a book is not found and retries the search
- **Smart Result Ranking** — book search results are now ranked by relevance (exact match, author, language)
- **Interactive Kindle Selection** — added inline buttons to choose which Kindle device to send the book to
- **Health Server Improvements** — replaced HTTPServer with ThreadingHTTPServer for better stability and added BrokenPipeError handling
- **GitHub Repos Endpoint** — added caching and GitHub token support to prevent rate limits on the Homepage dashboard
- **SMTP Fallback** — improved email delivery reliability with a fallback chain for SMTP passwords

### v4.1 (2026-04-16)
- **Apple Notes integration** — notes saved to Apple Notes via iCloud IMAP
- **Daily diary with chronography** — one note per day, each entry with timestamp
- **Book search via natural language** — "Find book..." triggers Flibusta OPDS search
- **Unified message routing** — all 6 types handled through Claude AI analysis
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
