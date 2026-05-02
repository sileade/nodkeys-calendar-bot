"""
Kindle Handler Module for Nodkeys Calendar Bot
Handles document files sent via Telegram, detects format via AI,
converts if necessary using Calibre, and sends to Kindle via iCloud SMTP.
"""

import os
import re
import json
import hashlib
import logging
import smtplib
import subprocess
# tempfile and mimetypes removed — unused
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path
from typing import Optional, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# ──────────────────── Configuration ────────────────────

KINDLE_EMAIL = os.getenv("KINDLE_EMAIL", "vera_muhamedova_abyH2D@kindle.com")
KINDLE_DEVICES = os.getenv("KINDLE_DEVICES", "")  # "Name1:email1|Name2:email2"
SMTP_HOST = os.getenv("KINDLE_SMTP_HOST", "smtp.mail.me.com")
SMTP_PORT = int(os.getenv("KINDLE_SMTP_PORT", "587"))
EMAIL_FROM = os.getenv("KINDLE_EMAIL_FROM", "slilea@icloud.com")
EMAIL_PASSWORD = os.getenv("KINDLE_EMAIL_PASSWORD", "") or os.getenv("SMTP_PASSWORD", "") or os.getenv("ICLOUD_PASSWORD", "")
SMTP_LOGIN = os.getenv("ICLOUD_USERNAME", EMAIL_FROM)  # Use iCloud username for SMTP auth
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TMP_DIR = os.getenv("KINDLE_TMP_DIR", "/tmp/kindle_files")
BOOKS_STORAGE_DIR = os.getenv("KINDLE_BOOKS_STORAGE", "/app/data/books")

# ──────────────────── S3 Configuration ────────────────────
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BOOKS_BUCKET = os.getenv("S3_BOOKS_BUCKET", "books")


def _get_s3_client():
    """Get S3 client for book storage."""
    if not S3_ENDPOINT_URL or not S3_ACCESS_KEY:
        return None
    try:
        import boto3
        from botocore.config import Config
        return boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name="us-east-1",
            config=Config(signature_version="s3v4"),
        )
    except Exception as e:
        logger.error("S3 client error: %s", e)
        return None


def _book_hash(filename: str, file_size: int) -> str:
    """Generate a unique hash for a book based on filename and size."""
    key = f"{filename}:{file_size}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _upload_book_to_s3(book_hash: str, file_path: str, filename: str) -> str:
    """Upload a book file to S3. Returns S3 key or empty string."""
    s3 = _get_s3_client()
    if not s3:
        return ""
    try:
        s3_key = f"{book_hash}/{filename}"
        s3.upload_file(file_path, S3_BOOKS_BUCKET, s3_key)
        logger.info("Book uploaded to S3: %s", s3_key)
        return s3_key
    except Exception as e:
        logger.error("S3 book upload error: %s", e)
        return ""


def _download_book_from_s3(s3_key: str, local_path: str) -> bool:
    """Download a book file from S3."""
    s3 = _get_s3_client()
    if not s3:
        return False
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(S3_BOOKS_BUCKET, s3_key, local_path)
        return True
    except Exception as e:
        logger.error("S3 book download error: %s", e)
        return False


def _check_book_s3_cache(book_hash: str) -> list:
    """Check if book files exist in S3 cache. Returns list of S3 keys."""
    s3 = _get_s3_client()
    if not s3:
        return []
    try:
        resp = s3.list_objects_v2(Bucket=S3_BOOKS_BUCKET, Prefix=f"{book_hash}/")
        contents = resp.get('Contents', [])
        return [obj['Key'] for obj in contents] if contents else []
    except Exception as e:
        logger.error("S3 book cache check error: %s", e)
        return []


def _ensure_books_bucket():
    """Ensure the books S3 bucket exists."""
    s3 = _get_s3_client()
    if not s3:
        return
    try:
        s3.head_bucket(Bucket=S3_BOOKS_BUCKET)
    except Exception:
        try:
            s3.create_bucket(Bucket=S3_BOOKS_BUCKET)
            logger.info("Created S3 bucket: %s", S3_BOOKS_BUCKET)
        except Exception as e:
            logger.warning("Could not create bucket %s: %s", S3_BOOKS_BUCKET, e)


# Formats Kindle accepts directly (no conversion needed)
KINDLE_NATIVE_FORMATS = {
    ".epub", ".pdf", ".txt", ".doc", ".docx",
    ".rtf", ".htm", ".html", ".png", ".jpg",
    ".jpeg", ".gif", ".bmp",
}

# Formats that Calibre can convert
CALIBRE_SUPPORTED = {
    ".fb2", ".mobi", ".azw", ".azw3", ".azw4",
    ".cbz", ".cbr", ".cb7", ".cbc",
    ".chm", ".djvu", ".docx", ".epub",
    ".fb2", ".htmlz", ".lit", ".lrf",
    ".mobi", ".odt", ".pdb", ".pdf",
    ".pml", ".rb", ".rtf", ".snb",
    ".tcr", ".txt", ".txtz",
}

# Best output format for Kindle
DEFAULT_OUTPUT_FORMAT = "epub"

# Stats
_kindle_stats = {"sent": 0, "converted": 0, "errors": 0}

# Books history - persistent list of all sent books
BOOKS_DB_PATH = os.getenv("KINDLE_BOOKS_DB", "/app/data/kindle_books.json")
_books_history = []


def _load_books_history():
    """Load books history from JSON file."""
    global _books_history
    try:
        os.makedirs(os.path.dirname(BOOKS_DB_PATH), exist_ok=True)
        if os.path.exists(BOOKS_DB_PATH):
            with open(BOOKS_DB_PATH, 'r', encoding='utf-8') as f:
                _books_history = json.load(f)
    except Exception as e:
        logger.error("Failed to load books history: %s", e)
        _books_history = []


def _save_books_history():
    """Save books history to JSON file."""
    try:
        os.makedirs(os.path.dirname(BOOKS_DB_PATH), exist_ok=True)
        with open(BOOKS_DB_PATH, 'w', encoding='utf-8') as f:
            json.dump(_books_history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save books history: %s", e)


def store_book_file(file_path: str, book_id: int, book_hash: str = "") -> str:
    """Copy book file to permanent storage and upload to S3. Returns stored path."""
    try:
        import shutil
        os.makedirs(BOOKS_STORAGE_DIR, exist_ok=True)
        ext = Path(file_path).suffix
        stored_name = f"{book_id:04d}_{Path(file_path).stem}{ext}"
        stored_path = os.path.join(BOOKS_STORAGE_DIR, stored_name)
        shutil.copy2(file_path, stored_path)
        logger.info("Book stored locally: %s", stored_path)
        
        # Upload to S3
        if book_hash:
            s3_key = _upload_book_to_s3(book_hash, file_path, os.path.basename(file_path))
            if s3_key:
                logger.info("Book stored in S3: %s", s3_key)
        
        return stored_name
    except Exception as e:
        logger.error("Failed to store book: %s", e)
        return ""


def add_book_to_history(filename: str, title: str, author: str, 
                        format_from: str, format_to: str,
                        kindle_email: str, converted: bool, 
                        file_size: int, stored_file: str = "",
                        book_hash: str = "", s3_key: str = ""):
    """Add a book entry to the history."""
    from datetime import datetime
    entry = {
        "id": len(_books_history) + 1,
        "filename": filename,
        "title": title or os.path.splitext(filename)[0],
        "author": author or "Unknown",
        "format_from": format_from,
        "format_to": format_to if converted else format_from,
        "converted": converted,
        "kindle_email": kindle_email,
        "file_size_kb": round(file_size / 1024),
        "sent_at": datetime.now().isoformat(),
        "status": "sent",
        "stored_file": stored_file,
        "book_hash": book_hash,
        "s3_key": s3_key,
    }
    _books_history.append(entry)
    _save_books_history()
    return entry


def get_books_history() -> list:
    """Return full books history."""
    if not _books_history:
        _load_books_history()
    return _books_history


# Load history on module import
_load_books_history()

# Ensure S3 bucket exists on module import
_ensure_books_bucket()


def get_kindle_devices() -> list:
    """Parse KINDLE_DEVICES env var into list of (name, email) tuples."""
    devices = []
    if KINDLE_DEVICES:
        for device in KINDLE_DEVICES.split("|"):
            parts = device.strip().split(":")
            if len(parts) == 2:
                devices.append((parts[0].strip(), parts[1].strip()))
    if not devices and KINDLE_EMAIL:
        devices.append(("Kindle", KINDLE_EMAIL))
    return devices


def get_file_extension(filename: str) -> str:
    """Get lowercase file extension."""
    return Path(filename).suffix.lower()


def is_kindle_native(ext: str) -> bool:
    """Check if format is natively supported by Kindle."""
    return ext in KINDLE_NATIVE_FORMATS


def is_convertible(ext: str) -> bool:
    """Check if format can be converted by Calibre."""
    return ext in CALIBRE_SUPPORTED


async def analyze_file_with_ai(filename: str, file_ext: str, file_size: int) -> dict:
    """Use AI to analyze the file and recommend best format for Kindle."""
    if not OPENAI_API_KEY:
        return {
            "format_detected": file_ext.lstrip(".").upper(),
            "recommendation": "epub" if not is_kindle_native(file_ext) else "direct",
            "needs_conversion": not is_kindle_native(file_ext),
            "description": f"Файл {file_ext.upper()} ({file_size / 1024:.0f} KB)",
        }

    try:
        from openai import OpenAI
        client = OpenAI()

        prompt = f"""Analyze this ebook/document file and provide recommendations for Kindle delivery.

File: {filename}
Extension: {file_ext}
Size: {file_size} bytes ({file_size / 1024:.1f} KB)

Kindle natively supports: EPUB, PDF, TXT, DOC, DOCX, RTF, HTM, HTML, PNG, JPG, GIF, BMP
Calibre can convert from: FB2, MOBI, AZW, AZW3, DJVU, CBZ, CBR, CHM, LIT, LRF, ODT, PDB, PML, RB, SNB, TCR

Respond in JSON:
{{
    "format_detected": "detected format name",
    "is_ebook": true/false,
    "language_guess": "language of the book if detectable from filename",
    "title_guess": "book title if detectable from filename",
    "author_guess": "author if detectable from filename",
    "needs_conversion": true/false,
    "recommended_output": "epub or pdf or direct",
    "reason": "brief explanation in Russian",
    "confidence": 0.0-1.0
}}"""

        response = client.chat.completions.create(
            model="gpt-4.1-nano",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=500,
        )

        result = json.loads(response.choices[0].message.content)
        return result

    except Exception as e:
        logger.error("AI analysis failed: %s", e)
        return {
            "format_detected": file_ext.lstrip(".").upper(),
            "needs_conversion": not is_kindle_native(file_ext),
            "recommended_output": "epub" if not is_kindle_native(file_ext) else "direct",
            "reason": "AI анализ недоступен, используем стандартную логику",
            "confidence": 0.5,
        }


def convert_with_calibre(input_path: str, output_format: str = "epub") -> Optional[str]:
    """Convert file using Calibre's ebook-convert."""
    try:
        input_p = Path(input_path)
        output_path = str(input_p.with_suffix(f".{output_format}"))

        cmd = [
            "ebook-convert",
            input_path,
            output_path,
            "--output-profile", "kindle_oasis",
        ]

        logger.info("Converting: %s -> %s", input_path, output_path)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min timeout
        )

        if result.returncode == 0 and os.path.exists(output_path):
            logger.info("Conversion successful: %s", output_path)
            _kindle_stats["converted"] += 1
            return output_path
        else:
            logger.error("Conversion failed: %s", result.stderr[:500])
            return None

    except subprocess.TimeoutExpired:
        logger.error("Conversion timed out for: %s", input_path)
        return None
    except FileNotFoundError:
        logger.error("ebook-convert not found. Is Calibre installed?")
        return None
    except Exception as e:
        logger.error("Conversion error: %s", e)
        return None


def extract_metadata(file_path: str) -> dict:
    """Extract book metadata using Calibre's ebook-meta."""
    metadata = {}
    try:
        result = subprocess.run(
            ["ebook-meta", file_path],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if ":" in line:
                    key, _, value = line.partition(":")
                    key = key.strip().lower()
                    value = value.strip()
                    if key == "title" and value:
                        metadata["title"] = value
                    elif key in ("author(s)", "authors") and value:
                        metadata["author"] = value
                    elif key == "languages" and value:
                        metadata["language"] = value
                    elif key == "publisher" and value:
                        metadata["publisher"] = value
    except Exception as e:
        logger.debug("Metadata extraction failed: %s", e)
    return metadata


def send_email_to_kindle(
    file_path: str,
    kindle_email: str,
    subject: str = "Kindle Book",
) -> Tuple[bool, str]:
    """Send file to Kindle via iCloud SMTP."""
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = kindle_email
        msg["Subject"] = subject

        # Add body
        body = MIMEText("Sent from Nodkeys Kindle Bot", "plain", "utf-8")
        msg.attach(body)

        # Attach file
        filename = os.path.basename(file_path)
        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{filename}"',
            )
            msg.attach(part)

        # Send via SMTP
        logger.info("Sending to %s via %s:%d", kindle_email, SMTP_HOST, SMTP_PORT)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_LOGIN, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, kindle_email, msg.as_string())

        _kindle_stats["sent"] += 1
        logger.info("Email sent successfully to %s", kindle_email)
        return True, "OK"

    except smtplib.SMTPAuthenticationError as e:
        logger.error("SMTP auth failed: %s", e)
        _kindle_stats["errors"] += 1
        return False, f"Ошибка авторизации SMTP: {e}"
    except smtplib.SMTPException as e:
        logger.error("SMTP error: %s", e)
        _kindle_stats["errors"] += 1
        return False, f"Ошибка SMTP: {e}"
    except Exception as e:
        logger.error("Email send error: %s", e)
        _kindle_stats["errors"] += 1
        return False, f"Ошибка: {e}"


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and special chars."""
    # Remove path components
    filename = os.path.basename(filename)
    # Remove potentially dangerous characters
    filename = re.sub(r'[^\w\s\-\.\(\)\[\]]', '_', filename)
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200 - len(ext)] + ext
    return filename


# ──────────────────── Telegram Handlers ────────────────────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle document files sent to the bot."""
    if not update.message or not update.message.document:
        return

    doc = update.message.document
    filename = sanitize_filename(doc.file_name or "unknown")
    file_ext = get_file_extension(filename)
    file_size = doc.file_size or 0

    # ── Kindle Clippings Detection ──
    original_name = (doc.file_name or "").lower()
    if "clipping" in original_name or "my clippings" in original_name:
        thinking_msg = await update.message.reply_text(
            "📎 <b>Обнаружен Kindle Clippings!</b>\n"
            "🔄 Анализирую цитаты...",
            parse_mode="HTML",
        )
        try:
            os.makedirs(TMP_DIR, exist_ok=True)
            tg_file = await context.bot.get_file(doc.file_id)
            local_path = os.path.join(TMP_DIR, filename)
            await tg_file.download_to_drive(local_path)
            
            with open(local_path, "r", encoding="utf-8-sig") as f:
                clippings_text = f.read()
            
            # Import parser from bot.py
            import sys
            bot_module = sys.modules.get("__main__")
            if bot_module and hasattr(bot_module, "parse_kindle_clippings"):
                books = bot_module.parse_kindle_clippings(clippings_text)
            else:
                # Inline fallback parser
                books = {}
                for entry in clippings_text.split("=========="):
                    entry = entry.strip()
                    if not entry:
                        continue
                    lines = entry.split("\n")
                    if len(lines) < 3:
                        continue
                    title = lines[0].strip().lstrip("\ufeff")
                    hl = []
                    found = False
                    for line in lines[1:]:
                        if not found and line.strip() == "":
                            found = True
                            continue
                        if found and line.strip():
                            hl.append(line.strip())
                    highlight = " ".join(hl).strip()
                    if highlight and title:
                        books.setdefault(title, [])
                        if highlight not in books[title]:
                            books[title].append(highlight)
            
            if not books:
                await thinking_msg.edit_text("❌ Не удалось распарсить цитаты из файла.")
                return
            
            total = sum(len(v) for v in books.values())
            await thinking_msg.edit_text(
                f"📚 Найдено {total} цитат из {len(books)} книг.\n"
                f"🧠 Генерирую анализ..."
            )
            
            if bot_module and hasattr(bot_module, "summarize_clippings_with_ai"):
                import asyncio
                result = await asyncio.to_thread(bot_module.summarize_clippings_with_ai, books)
            else:
                result = "📚 <b>Kindle Clippings</b>\n\n"
                for title, highlights in sorted(books.items(), key=lambda x: -len(x[1])):
                    result += f"\u2022 <b>{title}</b> \u2014 {len(highlights)} цитат\n"
            
            await thinking_msg.edit_text(result, parse_mode="HTML")
        except Exception as e:
            logger.error("Clippings parse error: %s", e)
            await thinking_msg.edit_text(f"❌ Ошибка анализа Clippings: {str(e)[:200]}")
        finally:
            try:
                for f in os.listdir(TMP_DIR):
                    fp = os.path.join(TMP_DIR, f)
                    if os.path.isfile(fp):
                        os.remove(fp)
            except Exception:
                pass
        return

    # Check if it's a supported format
    if not is_kindle_native(file_ext) and not is_convertible(file_ext):
        await update.message.reply_text(
            f"❌ Формат <b>{file_ext}</b> не поддерживается.\n\n"
            f"📚 Поддерживаемые форматы:\n"
            f"<b>Прямая отправка:</b> EPUB, PDF, TXT, DOC, DOCX, RTF, HTML\n"
            f"<b>С конвертацией:</b> FB2, MOBI, AZW, AZW3, DJVU, CBZ, CBR, CHM, LIT, ODT",
            parse_mode="HTML",
        )
        return

    # Send thinking message
    thinking_msg = await update.message.reply_text(
        f"📖 Анализирую файл <b>{filename}</b>...\n"
        f"📊 Размер: {file_size / 1024:.0f} KB",
        parse_mode="HTML",
    )

    # AI Analysis
    ai_result = await analyze_file_with_ai(filename, file_ext, file_size)

    # Build analysis message
    title_guess = ai_result.get("title_guess", "")
    author_guess = ai_result.get("author_guess", "")
    needs_conversion = ai_result.get("needs_conversion", not is_kindle_native(file_ext))
    recommended = ai_result.get("recommended_output", DEFAULT_OUTPUT_FORMAT)
    reason = ai_result.get("reason", "")
    confidence = ai_result.get("confidence", 0.5)

    conf_emoji = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.5 else "🔴"

    info_text = "📖 <b>Анализ файла</b>\n\n"
    info_text += f"📄 <b>Файл:</b> {filename}\n"
    info_text += f"📊 <b>Формат:</b> {ai_result.get('format_detected', file_ext.upper())}\n"
    info_text += f"📏 <b>Размер:</b> {file_size / 1024:.0f} KB\n"
    if title_guess:
        info_text += f"📕 <b>Название:</b> {title_guess}\n"
    if author_guess:
        info_text += f"✍️ <b>Автор:</b> {author_guess}\n"
    if needs_conversion:
        info_text += f"\n🔄 <b>Конвертация:</b> {file_ext.upper()} → {recommended.upper()}\n"
    else:
        info_text += "\n✅ <b>Формат поддерживается Kindle напрямую</b>\n"
    if reason:
        info_text += f"💡 <i>{reason}</i>\n"
    info_text += f"\n{conf_emoji} Уверенность AI: {confidence:.0%}"

    # Create keyboard with device options
    devices = get_kindle_devices()
    keyboard = []
    for name, email in devices:
        callback_data = f"kindle:{email}"
        keyboard.append([InlineKeyboardButton(f"📱 {name}", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="kindle:cancel")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await thinking_msg.edit_text(
        info_text + "\n\n📱 <b>Выберите устройство для отправки:</b>",
        parse_mode="HTML",
        reply_markup=reply_markup,
    )

    # Generate book hash for S3 caching
    bhash = _book_hash(filename, file_size)

    # Store file info in context for callback
    context.user_data["kindle_file"] = {
        "file_id": doc.file_id,
        "filename": filename,
        "file_ext": file_ext,
        "file_size": file_size,
        "needs_conversion": needs_conversion,
        "recommended_output": recommended if recommended != "direct" else None,
        "ai_result": ai_result,
        "book_hash": bhash,
    }


async def callback_kindle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Kindle device selection callback."""
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("kindle:"):
        return

    kindle_email = data[7:]  # Remove "kindle:" prefix

    if kindle_email == "cancel":
        await query.edit_message_text("❌ Отправка отменена.")
        context.user_data.pop("kindle_file", None)
        return

    file_info = context.user_data.get("kindle_file")
    if not file_info:
        await query.edit_message_text("⚠️ Файл не найден. Отправьте файл заново.")
        return

    # Update status
    await query.edit_message_text(
        f"⏳ Скачиваю и обрабатываю файл...\n"
        f"📄 {file_info['filename']}",
        parse_mode="HTML",
    )

    # Create temp directory
    os.makedirs(TMP_DIR, exist_ok=True)

    try:
        # Download file from Telegram
        tg_file = await context.bot.get_file(file_info["file_id"])
        local_path = os.path.join(TMP_DIR, file_info["filename"])
        await tg_file.download_to_drive(local_path)

        logger.info("Downloaded file: %s (%d bytes)", local_path, os.path.getsize(local_path))

        # Extract metadata if possible
        metadata = extract_metadata(local_path)
        title = metadata.get("title", file_info.get("ai_result", {}).get("title_guess", ""))
        author = metadata.get("author", file_info.get("ai_result", {}).get("author_guess", ""))

        send_path = local_path
        converted = False

        # Convert if needed
        if file_info["needs_conversion"] and file_info.get("recommended_output"):
            await query.edit_message_text(
                f"🔄 Конвертирую {file_info['file_ext'].upper()} → "
                f"{file_info['recommended_output'].upper()}...\n"
                f"📄 {file_info['filename']}",
                parse_mode="HTML",
            )

            converted_path = convert_with_calibre(
                local_path,
                file_info["recommended_output"],
            )

            if converted_path and os.path.exists(converted_path):
                send_path = converted_path
                converted = True
            else:
                # Try sending original if conversion fails
                if is_kindle_native(file_info["file_ext"]):
                    logger.warning("Conversion failed, sending original")
                else:
                    await query.edit_message_text(
                        f"❌ Ошибка конвертации.\n"
                        f"Формат {file_info['file_ext'].upper()} не удалось конвертировать.",
                    )
                    return

        # Send to Kindle
        await query.edit_message_text(
            f"📧 Отправляю на Kindle...\n"
            f"📄 {os.path.basename(send_path)}\n"
            f"📱 {kindle_email}",
            parse_mode="HTML",
        )

        subject = title if title else os.path.splitext(file_info["filename"])[0]
        success, error_msg = send_email_to_kindle(send_path, kindle_email, subject)

        if success:
            # Upload to S3 for persistent storage
            bhash = file_info.get("book_hash", "")
            s3_key = ""
            if bhash:
                s3_key = _upload_book_to_s3(bhash, send_path, os.path.basename(send_path))
                # Also upload original if converted
                if converted and local_path != send_path:
                    _upload_book_to_s3(bhash, local_path, os.path.basename(local_path))

            result_text = "\u2705 <b>\u041e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e \u043d\u0430 Kindle!</b>\n\n"
            result_text += f"\U0001f4c4 <b>\u0424\u0430\u0439\u043b:</b> {os.path.basename(send_path)}\n"
            if title:
                result_text += f"\U0001f4d5 <b>\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435:</b> {title}\n"
            if author:
                result_text += f"\u270d\ufe0f <b>\u0410\u0432\u0442\u043e\u0440:</b> {author}\n"
            result_text += f"\U0001f4f1 <b>\u0423\u0441\u0442\u0440\u043e\u0439\u0441\u0442\u0432\u043e:</b> {kindle_email}\n"
            if converted:
                result_text += f"\U0001f504 <b>\u041a\u043e\u043d\u0432\u0435\u0440\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u043e:</b> {file_info['file_ext'].upper()} \u2192 {file_info['recommended_output'].upper()}\n"
            if s3_key:
                result_text += f"\U0001f4be <b>S3:</b> \u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e \u0432 \u043e\u0431\u043b\u0430\u043a\u043e\n"
            result_text += "\n\U0001f4ec <i>\u041a\u043d\u0438\u0433\u0430 \u043f\u043e\u044f\u0432\u0438\u0442\u0441\u044f \u043d\u0430 Kindle \u0447\u0435\u0440\u0435\u0437 1-5 \u043c\u0438\u043d\u0443\u0442</i>"

            # Store book file permanently
            book_id = len(_books_history) + 1
            stored_file = store_book_file(send_path, book_id, book_hash=bhash)

            # Track in books history
            add_book_to_history(
                filename=file_info['filename'],
                title=title,
                author=author,
                format_from=file_info['file_ext'].upper(),
                format_to=file_info.get('recommended_output', '').upper() if converted else file_info['file_ext'].upper(),
                kindle_email=kindle_email,
                converted=converted,
                file_size=file_info['file_size'],
                stored_file=stored_file,
                book_hash=bhash,
                s3_key=s3_key,
            )

            await query.edit_message_text(result_text, parse_mode="HTML")
        else:
            await query.edit_message_text(
                f"❌ <b>Ошибка отправки</b>\n\n{error_msg}",
                parse_mode="HTML",
            )

    except Exception as e:
        logger.error("Kindle handler error: %s", e)
        await query.edit_message_text(
            f"❌ <b>Ошибка:</b> {str(e)[:200]}",
            parse_mode="HTML",
        )

    finally:
        # Cleanup temp files (but not stored books)
        try:
            for f in os.listdir(TMP_DIR):
                fp = os.path.join(TMP_DIR, f)
                if os.path.isfile(fp):
                    os.remove(fp)
        except Exception:
            pass

        context.user_data.pop("kindle_file", None)


def get_kindle_stats() -> dict:
    """Return Kindle handler statistics."""
    return {
        "kindle_sent": _kindle_stats["sent"],
        "kindle_converted": _kindle_stats["converted"],
        "kindle_errors": _kindle_stats["errors"],
    }


def get_book_history() -> list:
    """Return the full list of books sent to Kindle."""
    _load_books_history()
    return _books_history
