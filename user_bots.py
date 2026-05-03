"""
Nodkeys User Bots Module
Allows Pro+ users to connect their own Telegram bots.

How it works:
1. User creates a bot via @BotFather
2. User sends bot token to Nodkeys
3. Nodkeys starts polling the user's bot
4. Messages to user's bot are processed by Nodkeys engine
5. Responses are sent via user's bot

Architecture:
- Main bot runs as usual (polling)
- User bots run as separate polling tasks
- Each user bot has its own Application instance
- User bot handlers mirror main bot handlers
"""

import asyncio
import logging
from typing import Optional
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from user_store import get_custom_bot_token, get_user

logger = logging.getLogger(__name__)

# Active user bot instances: {user_id: Application}
_active_bots: dict[int, Application] = {}


async def start_user_bot(user_id: int, message_handler_func, callback_handler_func) -> bool:
    """Start polling for a user's custom bot.
    
    Args:
        user_id: owner's Telegram user ID
        message_handler_func: async function to handle messages
        callback_handler_func: async function to handle callbacks
    
    Returns:
        True if started successfully
    """
    token = get_custom_bot_token(user_id)
    if not token:
        logger.warning("No custom bot token for user %d", user_id)
        return False
    
    if user_id in _active_bots:
        logger.info("User bot already running for user %d", user_id)
        return True
    
    try:
        app = Application.builder().token(token).build()
        
        # Add handlers that route to main bot logic
        app.add_handler(CommandHandler("start", _make_start_handler(user_id)))
        app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            _make_message_handler(user_id, message_handler_func)
        ))
        app.add_handler(CallbackQueryHandler(
            _make_callback_handler(user_id, callback_handler_func)
        ))
        
        # Start polling in background
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        
        _active_bots[user_id] = app
        logger.info("Started custom bot for user %d", user_id)
        return True
        
    except Exception as e:
        logger.error("Failed to start custom bot for user %d: %s", user_id, e)
        return False


async def stop_user_bot(user_id: int):
    """Stop a user's custom bot."""
    if user_id not in _active_bots:
        return
    
    app = _active_bots.pop(user_id)
    try:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        logger.info("Stopped custom bot for user %d", user_id)
    except Exception as e:
        logger.error("Error stopping custom bot for user %d: %s", user_id, e)


async def restart_user_bot(user_id: int, message_handler_func, callback_handler_func):
    """Restart a user's custom bot (e.g., after token change)."""
    await stop_user_bot(user_id)
    await start_user_bot(user_id, message_handler_func, callback_handler_func)


def _make_start_handler(owner_id: int):
    """Create /start handler for user bot."""
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = get_user(owner_id)
        name = user.get("first_name", "Nodkeys") if user else "Nodkeys"
        await update.message.reply_text(
            f"👋 Привет! Это персональный бот {name}.\n\n"
            f"Напишите что-нибудь, и я обработаю ваш запрос."
        )
    return handler


def _make_message_handler(owner_id: int, process_func):
    """Create message handler that routes to main bot logic."""
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Only process messages from the owner
        if update.effective_user.id != owner_id:
            await update.message.reply_text(
                "⚠️ Этот бот настроен для личного использования."
            )
            return
        await process_func(update, context)
    return handler


def _make_callback_handler(owner_id: int, callback_func):
    """Create callback handler that routes to main bot logic."""
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != owner_id:
            await update.callback_query.answer("⚠️ Недоступно", show_alert=True)
            return
        await callback_func(update, context)
    return handler


async def start_all_user_bots(message_handler_func, callback_handler_func):
    """Start all configured user bots on application startup."""
    from user_store import _load_users
    users = _load_users()
    
    started = 0
    for uid, user in users.items():
        if user.get("custom_bot_token"):
            user_id = int(uid)
            success = await start_user_bot(user_id, message_handler_func, callback_handler_func)
            if success:
                started += 1
    
    if started:
        logger.info("Started %d custom user bots", started)


def get_active_bots_count() -> int:
    """Get number of active user bots."""
    return len(_active_bots)
