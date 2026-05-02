# Nodkeys Calendar Bot — Deploy & Rollback Guide v5.1

## Pre-Deploy Checklist

Before deploying v5.1, verify the following:

| Step | Command | Expected |
|------|---------|----------|
| Tests pass | `python3 test_new_features.py` | 36/36 passed |
| Syntax check | `python3 -m pyflakes bot.py kindle_handler.py ical_proxy.py` | No output |
| Docker build | `docker build -t calendar-bot:v5.1 .` | Success |
| `.env` updated | Check new vars in `.env.example` | All present |
| Group routing | Set `ALLOWED_CHAT_IDS` and `GROUP_USERS` in `.env` | Configured |

## Deploy Steps

### Step 1: Backup Current State

```bash
# SSH to server
ssh root@home.nodkeys.com

# Tag current working image
docker tag calendar-bot:latest calendar-bot:v4.2-backup

# Backup current source files
cd /root/quick-arr-Stack/calendar-bot
cp bot.py bot.py.v4.2.bak
cp kindle_handler.py kindle_handler.py.v4.2.bak
cp ical_proxy.py ical_proxy.py.v4.2.bak
cp requirements.txt requirements.txt.v4.2.bak

# Backup persistent data
cp -r /path/to/data /path/to/data.v4.2.bak
```

### Step 2: Deploy New Version

```bash
# Copy new files to server
scp bot.py kindle_handler.py ical_proxy.py requirements.txt \
    Dockerfile .env.example DEPLOY.md \
    root@home.nodkeys.com:/root/quick-arr-Stack/calendar-bot/

# Build new image
cd /root/quick-arr-Stack/calendar-bot
docker build -t calendar-bot:v5.1 .
docker tag calendar-bot:v5.1 calendar-bot:latest

# Restart container
docker compose down calendar-bot
docker compose up -d calendar-bot

# Verify
docker logs -f --tail 50 calendar-bot
```

### Step 3: Verify Deployment

```bash
# Check health endpoint
curl http://localhost:8085/health

# Expected response:
# {"status": "ok", "version": "5.1", ...}

# Check iCal proxy
curl http://localhost:8086/ | head -5

# Test bot commands in Telegram:
# /start — should show new features
# /help — should list X-Ray, URL→Kindle, Clippings
# /xray Мастер и Маргарита — should generate analysis
```

## Rollback Plan

### Quick Rollback (< 1 min)

If the bot crashes or behaves incorrectly after deploy:

```bash
# Stop the broken container
docker compose down calendar-bot

# Restore backup files
cd /root/quick-arr-Stack/calendar-bot
cp bot.py.v4.2.bak bot.py
cp kindle_handler.py.v4.2.bak kindle_handler.py
cp ical_proxy.py.v4.2.bak ical_proxy.py
cp requirements.txt.v4.2.bak requirements.txt

# Rebuild with old code
docker build -t calendar-bot:latest .
docker compose up -d calendar-bot

# Verify rollback
curl http://localhost:8085/health
# Should show "version": "4.2"
```

### Docker Image Rollback (instant)

If you tagged the backup image:

```bash
docker compose down calendar-bot
docker tag calendar-bot:v4.2-backup calendar-bot:latest
docker compose up -d calendar-bot
```

### Git Rollback

```bash
# Revert to previous commit
cd /root/quick-arr-Stack/calendar-bot
git log --oneline -5  # Find the v4.2 commit hash
git checkout <v4.2-commit-hash> -- bot.py kindle_handler.py ical_proxy.py requirements.txt

# Rebuild
docker build -t calendar-bot:latest .
docker compose up -d calendar-bot
```

## What Changed in v5.1

### New Features (safe — additive only)

| Feature | Files Changed | Risk |
|---------|--------------|------|
| Per-user calendar routing | `bot.py` (config + handle_message) | Low — no-op when GROUP_USERS empty |
| Multi-chat authorization | `bot.py` (ALLOWED_CHAT_IDS) | Low — backwards compatible |
| Sender context for Claude | `bot.py` (analyze_message) | Low — optional prompt append |
| X-Ray book analysis | `bot.py` (new function) | Low — isolated, uses Claude |
| URL → Kindle | `bot.py` (new function) | Low — isolated, uses existing Kindle pipeline |
| Kindle Clippings parser | `bot.py` + `kindle_handler.py` | Low — new handler in document flow |
| `/xray` command | `bot.py` (handler registration) | Low — new command, no conflicts |

### New Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `ALLOWED_CHAT_IDS` | Additional allowed chat IDs (comma-separated) | No |
| `GROUP_USERS` | Per-user routing: `username:name:rule\|...` | No |

**Example `.env` addition for group chat:**
```
ALLOWED_CHAT_IDS=-1001234567890
GROUP_USERS=vera_username:Вера:family|seleadi:Ilea:auto
```

### Bug Fixes (moderate risk)

| Fix | Files | Risk | Rollback Impact |
|-----|-------|------|-----------------|
| URL regex trailing punctuation | `bot.py` | Low | URL detection might differ |
| Unused imports removed | `bot.py`, `kindle_handler.py` | None | Cosmetic |
| f-string fixes | `kindle_handler.py` | None | Cosmetic |
| ical_proxy rewrite | `ical_proxy.py` | **Medium** | Calendar feed format may differ |

### Infrastructure Changes

| Change | File | Risk |
|--------|------|------|
| `beautifulsoup4` added | `requirements.txt` | Low — new dependency |
| HEALTHCHECK added | `Dockerfile` | Low — Docker feature |
| VOLUME directive | `Dockerfile` | Low — declarative |
| New env vars documented | `.env.example` | None — documentation only |

## Monitoring After Deploy

Check these within 15 minutes of deploy:

1. **Health endpoint**: `curl http://localhost:8085/health` — should return `"status": "ok"`
2. **Bot responds**: Send `/start` in Telegram — should show v5.1 features
3. **Group routing**: Send message from Vera in group — should create event in Family calendar
4. **Auto routing**: Send message from @seleadi in group — Claude should decide calendar
3. **Calendar works**: Send a message like "Встреча завтра в 15:00" — should create event
4. **Books work**: Send `/book Мастер и Маргарита` — should search Flibusta
5. **X-Ray works**: Send `/xray 1984` — should generate analysis
6. **Kindle works**: Send a `.epub` file — should offer device selection
7. **iCal proxy**: `curl http://localhost:8086/` — should return valid iCal data
8. **Docker logs**: `docker logs --tail 100 calendar-bot` — no ERROR lines

## Emergency Contacts

- **Status page**: https://status.nodkeys.com/
- **Dashboard**: https://home.nodkeys.com/
- **GitHub repo**: https://github.com/sileade/nodkeys-calendar-bot
