"""
Kindle Handler Module for Nodkeys Calendar Bot
Handles document files sent via Telegram, detects format via AI,
converts if necessary using Calibre, and sends to Kindle via iCloud SMTP.
"""

import os
import re
import json
import logging
import smtplib
import subprocess
import tempfile
import mimetypes
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
EMAIL_FROM = os.getenv("KINDLE_EMAIL_FROM", "home@sliadea.com")
EMAIL_PASSWORD = os.getenv("KINDLE_EMAIL_PASSWORD", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TMP_DIR = os.getenv("KINDLE_TMP_DIR", "/tmp/kindle_files")

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
            "reason": f"AI анализ недоступен, используем стандартную логику",
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
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
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

    info_text = f"📖 <b>Анализ файла</b>\n\n"
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
        info_text += f"\n✅ <b>Формат поддерживается Kindle напрямую</b>\n"
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

    # Store file info in context for callback
    context.user_data["kindle_file"] = {
        "file_id": doc.file_id,
        "filename": filename,
        "file_ext": file_ext,
        "file_size": file_size,
        "needs_conversion": needs_conversion,
        "recommended_output": recommended if recommended != "direct" else None,
        "ai_result": ai_result,
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
            result_text = f"✅ <b>Отправлено на Kindle!</b>\n\n"
            result_text += f"📄 <b>Файл:</b> {os.path.basename(send_path)}\n"
            if title:
                result_text += f"📕 <b>Название:</b> {title}\n"
            if author:
                result_text += f"✍️ <b>Автор:</b> {author}\n"
            result_text += f"📱 <b>Устройство:</b> {kindle_email}\n"
            if converted:
                result_text += f"🔄 <b>Конвертировано:</b> {file_info['file_ext'].upper()} → {file_info['recommended_output'].upper()}\n"
            result_text += f"\n📬 <i>Книга появится на Kindle через 1-5 минут</i>"

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
        # Cleanup temp files
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
