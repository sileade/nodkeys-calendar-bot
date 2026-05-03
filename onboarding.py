"""
Nodkeys Onboarding & Settings Module
Guides new users through setup and manages integration configuration.

Flow:
1. /start → Welcome + create user profile
2. Setup calendar (CalDAV URL + credentials)
3. Setup Kindle (email)
4. Setup preferences (timezone, language)
5. Done → show available commands

Settings can be changed later via /settings
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ContextTypes,
    ConversationHandler,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from user_store import (
    get_user, create_user, update_user,
    get_user_settings, save_user_settings,
    set_custom_bot_token, get_custom_bot_token,
    get_user_plan, is_subscription_active, PLANS,
)
from crypto import get_user_fingerprint

logger = logging.getLogger(__name__)

# Conversation states
(
    STATE_WELCOME,
    STATE_CALENDAR_URL,
    STATE_CALENDAR_USER,
    STATE_CALENDAR_PASS,
    STATE_KINDLE_EMAIL,
    STATE_TIMEZONE,
    STATE_BOT_TOKEN,
    STATE_DONE,
) = range(8)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start — welcome and onboarding."""
    user = update.effective_user
    user_id = user.id
    
    # Create user if not exists
    profile = get_user(user_id)
    if not profile:
        profile = create_user(user_id, 
                             username=user.username or "",
                             first_name=user.first_name or "")
    
    fingerprint = get_user_fingerprint(user_id)
    
    text = (
        f"👋 Привет, {user.first_name}!\n\n"
        f"Я — **Nodkeys Bot**, твой персональный ассистент.\n\n"
        f"🔐 Шифрование активно (ID: `{fingerprint}`)\n"
        f"Все твои данные зашифрованы — никто, включая разработчиков, не может их прочитать.\n\n"
        f"**Что я умею:**\n"
        f"📅 Управление календарём (Apple/Google/CalDAV)\n"
        f"📚 Поиск и прослушивание аудиокниг\n"
        f"📖 Отправка книг на Kindle\n"
        f"📝 Заметки и дневник\n"
        f"💰 Учёт финансов\n"
        f"⏰ Напоминания\n\n"
        f"Настроить интеграции? Нажмите кнопку ниже."
    )
    
    buttons = [
        [InlineKeyboardButton("⚙️ Настроить интеграции", callback_data="onb:setup")],
        [InlineKeyboardButton("📋 Все команды", callback_data="onb:commands")],
    ]
    
    if not profile.get("onboarding_complete"):
        buttons.insert(0, [InlineKeyboardButton("🚀 Быстрая настройка", callback_data="onb:quick")])
    
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /settings — show current settings and options to change."""
    user_id = update.effective_user.id
    profile = get_user(user_id)
    
    if not profile:
        await update.message.reply_text("Сначала используйте /start")
        return
    
    settings = get_user_settings(user_id)
    plan = get_user_plan(user_id)
    plan_name = PLANS.get(plan, {}).get("name", "Free")
    active = is_subscription_active(user_id)
    
    # Build status text
    cal = settings["integrations"]["calendar"]
    kindle = settings["integrations"]["kindle"]
    
    text = (
        f"⚙️ **Настройки**\n\n"
        f"**Подписка:** {plan_name} {'✅' if active else '(неактивна)'}\n"
        f"**Шифрование:** `{get_user_fingerprint(user_id)}`\n\n"
        f"**Интеграции:**\n"
        f"📅 Календарь: {'✅ Подключён' if cal['enabled'] else '❌ Не настроен'}\n"
        f"📖 Kindle: {'✅ ' + kindle['email'] if kindle['enabled'] else '❌ Не настроен'}\n"
        f"📝 Заметки: {'✅' if settings['integrations']['notes']['enabled'] else '❌'}\n\n"
        f"**Свой бот:** {'✅ Подключён' if get_custom_bot_token(user_id) else '❌ Не подключён'}\n"
    )
    
    buttons = [
        [
            InlineKeyboardButton("📅 Календарь", callback_data="set:calendar"),
            InlineKeyboardButton("📖 Kindle", callback_data="set:kindle"),
        ],
        [
            InlineKeyboardButton("📝 Заметки", callback_data="set:notes"),
            InlineKeyboardButton("🤖 Свой бот", callback_data="set:bot"),
        ],
        [
            InlineKeyboardButton("🌍 Часовой пояс", callback_data="set:timezone"),
            InlineKeyboardButton("💳 Подписка", callback_data="set:subscription"),
        ],
    ]
    
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")


async def callback_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle settings callback buttons."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = update.effective_user.id
    chat_id = query.message.chat_id
    
    if data == "onb:quick" or data == "onb:setup":
        text = (
            "🚀 **Настройка интеграций**\n\n"
            "Выберите что настроить:\n\n"
            "1️⃣ **Календарь** — для событий и задач\n"
            "2️⃣ **Kindle** — для отправки книг\n"
            "3️⃣ **Свой бот** — использовать свой Telegram бот\n"
            "4️⃣ **Пропустить** — настроить позже"
        )
        buttons = [
            [InlineKeyboardButton("📅 Настроить календарь", callback_data="set:calendar")],
            [InlineKeyboardButton("📖 Настроить Kindle", callback_data="set:kindle")],
            [InlineKeyboardButton("🤖 Подключить свой бот", callback_data="set:bot")],
            [InlineKeyboardButton("⏭ Пропустить", callback_data="onb:skip")],
        ]
        kb = InlineKeyboardMarkup(buttons)
        await query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    
    elif data == "onb:commands":
        text = (
            "📋 **Доступные команды:**\n\n"
            "/start — Приветствие и настройка\n"
            "/settings — Настройки и интеграции\n"
            "/subscribe — Управление подпиской\n"
            "/shortcuts — Быстрые команды для телефона\n"
            "/mybot — Подключить свой бот\n"
            "/help — Справка\n\n"
            "**Или просто напишите:**\n"
            "• «Встреча завтра в 15:00» → календарь\n"
            "• «Найди книгу Мастер и Маргарита» → поиск\n"
            "• «Найди аудиокнигу Война и мир» → аудио\n"
            "• «Напомни купить молоко» → напоминание\n"
        )
        await query.edit_message_text(text, parse_mode="Markdown")
    
    elif data == "onb:skip":
        update_user(user_id, onboarding_complete=True)
        await query.edit_message_text(
            "✅ Готово! Можете начинать пользоваться.\n\n"
            "Настроить интеграции можно позже через /settings"
        )
    
    elif data == "set:calendar":
        text = (
            "📅 **Настройка календаря**\n\n"
            "Отправьте данные в формате:\n"
            "```\n"
            "caldav_url\n"
            "email\n"
            "пароль_приложения\n"
            "```\n\n"
            "**Для Apple iCloud:**\n"
            "URL: `https://caldav.icloud.com/`\n"
            "Email: ваш Apple ID\n"
            "Пароль: [создайте пароль приложения](https://appleid.apple.com/)\n\n"
            "**Для Google:**\n"
            "URL: `https://www.googleapis.com/caldav/v2/`\n\n"
            "🔐 Данные будут зашифрованы."
        )
        context.user_data["awaiting"] = "calendar_setup"
        await query.edit_message_text(text, parse_mode="Markdown")
    
    elif data == "set:kindle":
        text = (
            "📖 **Настройка Kindle**\n\n"
            "Отправьте ваш Kindle email:\n"
            "Например: `yourname@kindle.com`\n\n"
            "Не забудьте добавить `bot@nodkeys.com` в \n"
            "[одобренные отправители](https://www.amazon.com/hz/mycd/myx#/home/settings/payment)\n\n"
            "🔐 Email будет зашифрован."
        )
        context.user_data["awaiting"] = "kindle_setup"
        await query.edit_message_text(text, parse_mode="Markdown")
    
    elif data == "set:bot":
        plan = get_user_plan(user_id)
        if plan != "pro_plus" and not is_subscription_active(user_id):
            text = (
                "🤖 **Подключение своего бота**\n\n"
                "Эта функция доступна на тарифе **Pro+** (499₽/мес).\n\n"
                "С своим ботом:\n"
                "• Бот работает от вашего имени\n"
                "• Свой username и аватар\n"
                "• Полный контроль\n\n"
                "Оформите подписку через /subscribe"
            )
            await query.edit_message_text(text, parse_mode="Markdown")
            return
        
        current = get_custom_bot_token(user_id)
        text = (
            "🤖 **Подключение своего бота**\n\n"
        )
        if current:
            text += "✅ Бот уже подключён.\n\n"
            text += "Отправьте новый токен для замены, или нажмите 'Отключить'.\n"
            buttons = [[InlineKeyboardButton("❌ Отключить бот", callback_data="set:bot:disconnect")]]
            kb = InlineKeyboardMarkup(buttons)
            await query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            text += (
                "Отправьте токен бота от @BotFather:\n"
                "Например: `123456:ABCdef...`\n\n"
                "🔐 Токен будет зашифрован."
            )
            context.user_data["awaiting"] = "bot_token_setup"
            await query.edit_message_text(text, parse_mode="Markdown")
    
    elif data == "set:bot:disconnect":
        update_user(user_id, custom_bot_token=None)
        await query.edit_message_text("✅ Бот отключён. Используется основной бот Nodkeys.")
    
    elif data == "set:timezone":
        text = (
            "🌍 **Часовой пояс**\n\n"
            "Отправьте ваш часовой пояс:\n"
            "Например: `Europe/Moscow`, `Asia/Yekaterinburg`, `Europe/Kaliningrad`"
        )
        context.user_data["awaiting"] = "timezone_setup"
        await query.edit_message_text(text, parse_mode="Markdown")
    
    elif data == "set:subscription":
        # Redirect to subscribe command
        from subscription import cmd_subscribe
        # Fake a message for cmd_subscribe
        await query.edit_message_text("Загрузка...")
        await cmd_subscribe(update, context)


async def handle_settings_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle text input during settings setup.
    
    Returns True if message was consumed, False if not a settings input.
    """
    awaiting = context.user_data.get("awaiting")
    if not awaiting:
        return False
    
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    if awaiting == "calendar_setup":
        lines = text.split("\n")
        if len(lines) < 3:
            await update.message.reply_text(
                "❌ Нужно 3 строки:\n"
                "1. CalDAV URL\n"
                "2. Email/логин\n"
                "3. Пароль приложения"
            )
            return True
        
        settings = get_user_settings(user_id)
        settings["integrations"]["calendar"] = {
            "enabled": True,
            "caldav_url": lines[0].strip(),
            "username": lines[1].strip(),
            "password": lines[2].strip(),
            "calendars": {},
        }
        save_user_settings(user_id, settings)
        
        # Delete the message with credentials
        try:
            await update.message.delete()
        except Exception:
            pass
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✅ Календарь настроен! Данные зашифрованы и сохранены.\n\n"
                 "Теперь вы можете создавать события, просто написав мне."
        )
        context.user_data.pop("awaiting", None)
        return True
    
    elif awaiting == "kindle_setup":
        if "@" not in text:
            await update.message.reply_text("❌ Это не похоже на email. Попробуйте ещё раз.")
            return True
        
        settings = get_user_settings(user_id)
        settings["integrations"]["kindle"] = {
            "enabled": True,
            "email": text,
        }
        save_user_settings(user_id, settings)
        
        await update.message.reply_text(
            f"✅ Kindle настроен: `{text}`\n\n"
            f"Теперь можете отправлять книги командой «отправь на Kindle».",
            parse_mode="Markdown"
        )
        context.user_data.pop("awaiting", None)
        return True
    
    elif awaiting == "bot_token_setup":
        if ":" not in text or len(text) < 20:
            await update.message.reply_text("❌ Это не похоже на токен бота. Формат: `123456:ABCdef...`")
            return True
        
        set_custom_bot_token(user_id, text)
        
        # Delete message with token
        try:
            await update.message.delete()
        except Exception:
            pass
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="✅ Бот подключён! Токен зашифрован.\n\n"
                 "Ваш бот будет использоваться для отправки сообщений."
        )
        context.user_data.pop("awaiting", None)
        return True
    
    elif awaiting == "timezone_setup":
        # Validate timezone
        from zoneinfo import ZoneInfo
        try:
            ZoneInfo(text)
        except Exception:
            await update.message.reply_text(
                "❌ Неверный часовой пояс. Примеры:\n"
                "`Europe/Moscow`, `Asia/Novosibirsk`, `Europe/London`",
                parse_mode="Markdown"
            )
            return True
        
        settings = get_user_settings(user_id)
        settings["preferences"]["timezone"] = text
        save_user_settings(user_id, settings)
        
        await update.message.reply_text(f"✅ Часовой пояс: `{text}`", parse_mode="Markdown")
        context.user_data.pop("awaiting", None)
        return True
    
    return False
