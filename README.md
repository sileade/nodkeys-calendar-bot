# Nodkeys Calendar & Kindle Bot v3.0

A powerful, self-hosted Telegram bot that combines **AI-powered calendar management** with **smart Kindle document delivery**. Built for the Nodkeys home infrastructure.

## Features

### Calendar Management

The bot analyzes forwarded Telegram messages using **Claude AI** and automatically creates events, tasks, and reminders in Apple Calendar via iCloud CalDAV.

| Feature | Description |
|---------|-------------|
| **AI Analysis** | Claude AI parses natural language to extract dates, times, and event types |
| **Auto-detect** | Distinguishes between events, tasks, and reminders |
| **URL Detection** | Automatically creates "Review" tasks for shared links |
| **Multi-calendar** | Routes entries to Work, Family, or Reminders calendars |
| **Smart Dates** | Understands "tomorrow", "next Friday", "in a week", etc. |
| **Delete/Cleanup** | Remove individual events or bulk-clean bot-created entries |
| **iCal Proxy** | Built-in proxy server for Homepage calendar widget integration |

### Kindle Document Delivery

Send any ebook or document to the bot, and it will analyze the format, convert if necessary, and deliver it to your Kindle device via email.

| Feature | Description |
|---------|-------------|
| **AI Format Detection** | OpenAI analyzes the file and recommends the best format |
| **Auto Conversion** | Calibre converts unsupported formats to EPUB |
| **Direct Send** | EPUB, PDF, TXT, DOC, DOCX, RTF, HTML sent without conversion |
| **Metadata Extraction** | Shows book title, author, and language before sending |
| **Multi-device** | Support for multiple Kindle devices with interactive selection |
| **iCloud SMTP** | Sends via iCloud Mail for reliable delivery |

### Supported Formats

**Direct send (no conversion):** EPUB, PDF, TXT, DOC, DOCX, RTF, HTM, HTML, PNG, JPG, GIF, BMP

**Conversion via Calibre:** FB2, MOBI, AZW, AZW3, AZW4, CBZ, CBR, CB7, CHM, DJVU, LIT, LRF, ODT, PDB, PML, RB, SNB, TCR

## Architecture

```
Telegram Bot (python-telegram-bot)
├── Calendar Handler (Claude AI + CalDAV)
│   ├── Message Analysis (Anthropic Claude)
│   ├── Event Creation (iCloud CalDAV)
│   └── iCal Proxy Server (:8086)
├── Kindle Handler (OpenAI + Calibre + SMTP)
│   ├── AI Format Analysis (OpenAI GPT-4.1-nano)
│   ├── Format Conversion (Calibre ebook-convert)
│   └── Email Delivery (iCloud SMTP)
└── Health Check Server (:8085)
```

## Quick Start

### Prerequisites

Docker and Docker Compose installed on your server.

### 1. Clone the repository

```bash
git clone https://github.com/iLea/nodkeys-calendar-bot.git
cd nodkeys-calendar-bot
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Build and run

```bash
docker build -t calendar-kindle-bot:latest .
docker run -d \
  --name calendar-bot \
  --restart unless-stopped \
  --env-file .env \
  -p 8085:8085 \
  -p 8086:8086 \
  calendar-kindle-bot:latest
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|:--------:|
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather | **Yes** |
| `TELEGRAM_CHAT_ID` | Allowed Telegram chat ID | **Yes** |
| `CLAUDE_API_KEY` | Anthropic Claude API key | **Yes** |
| `CLAUDE_MODEL` | Claude model name | No |
| `ICLOUD_USERNAME` | iCloud email for CalDAV | **Yes** |
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

### Multi-Kindle Device Setup

```
KINDLE_DEVICES="My Kindle:email1@kindle.com|Family Kindle:email2@kindle.com"
```

## Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome message and bot info |
| `/help` | Show available commands |
| `/today` | List today's calendar events |
| `/calendars` | Show configured calendars |
| `/delete` | Search and delete events |
| `/cleanup` | Remove all bot-created events |

### Usage

**Calendar:** Forward any message or type naturally — the bot will analyze it and create a calendar entry.

**Kindle:** Send any document file (EPUB, FB2, PDF, etc.) — the bot will analyze it, show metadata, and offer to send it to your Kindle.

## API Endpoints

### Health Check

```
GET http://localhost:8085/health
```

Response:
```json
{
  "status": "ok",
  "bot": "Nodkeys Calendar & Kindle Bot v3.0",
  "uptime_seconds": 3600,
  "messages_processed": 42,
  "kindle_sent": 5,
  "kindle_converted": 3,
  "kindle_errors": 0
}
```

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
    description: Calendar & Kindle Bot
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
|-----------|-----------|
| Language | Python 3.12 |
| Bot Framework | python-telegram-bot 22.x |
| Calendar AI | Anthropic Claude (Sonnet) |
| Format AI | OpenAI GPT-4.1-nano |
| Calendar | iCloud CalDAV |
| Conversion | Calibre (ebook-convert) |
| Email | SMTP (iCloud Mail) |
| Container | Docker |

## License

MIT License. See [LICENSE](LICENSE) for details.
