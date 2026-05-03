"""
Nodkeys Subscription Module
Handles payments via Telegram Stars and YooKassa.

Telegram Stars:
- Built-in Telegram currency
- No external payment provider needed
- Available in Russia via SberPay
- Uses sendInvoice with provider_token="" and currency="XTR"

YooKassa:
- Direct card payments in Russia
- Requires provider_token from @BotFather (linked to YooKassa account)
- Uses sendInvoice with provider_token and currency="RUB"
"""

import os
import logging
from telegram import Update, LabeledPrice, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from user_store import (
    get_user, create_user, activate_subscription, get_user_plan,
    is_subscription_active, PLANS
)

logger = logging.getLogger(__name__)

# YooKassa provider token (from @BotFather)
YOOKASSA_TOKEN = os.environ.get("YOOKASSA_PROVIDER_TOKEN", "")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /subscribe command — show subscription options."""
    user_id = update.effective_user.id
    user = get_user(user_id)
    if not user:
        create_user(user_id, 
                   username=update.effective_user.username or "",
                   first_name=update.effective_user.first_name or "")
    
    current_plan = get_user_plan(user_id)
    is_active = is_subscription_active(user_id)
    
    text = "💳 **Подписка Nodkeys**\n\n"
    
    if is_active:
        sub = get_user(user_id).get("subscription", {})
        text += f"✅ Текущий план: **{PLANS[current_plan]['name']}**\n"
        text += f"📅 Активна до: {sub.get('active_until', 'N/A')[:10]}\n\n"
    else:
        text += f"📌 Текущий план: **Free**\n\n"
    
    text += "**Тарифы:**\n\n"
    text += "🆓 **Free** — бесплатно\n"
    text += "• 1 аудиокнига/мес, 3 задачи/день, 3 отправки на Kindle/мес\n\n"
    text += "⭐ **Pro** — 299₽/мес (150 Stars)\n"
    text += "• Всё без ограничений\n\n"
    text += "🌟 **Pro+** — 499₽/мес (250 Stars)\n"
    text += "• Pro + свой бот + приоритет\n"
    
    buttons = []
    
    # Telegram Stars buttons
    buttons.append([
        InlineKeyboardButton("⭐ Pro (150 Stars)", callback_data="sub:stars:pro"),
        InlineKeyboardButton("🌟 Pro+ (250 Stars)", callback_data="sub:stars:pro_plus"),
    ])
    
    # YooKassa buttons (if configured)
    if YOOKASSA_TOKEN:
        buttons.append([
            InlineKeyboardButton("💳 Pro (299₽)", callback_data="sub:yookassa:pro"),
            InlineKeyboardButton("💳 Pro+ (499₽)", callback_data="sub:yookassa:pro_plus"),
        ])
    
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")


async def callback_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle subscription callback buttons."""
    query = update.callback_query
    await query.answer()
    
    data = query.data  # sub:stars:pro or sub:yookassa:pro_plus
    parts = data.split(":")
    if len(parts) != 3:
        return
    
    _, method, plan = parts
    user_id = update.effective_user.id
    chat_id = query.message.chat_id
    
    if plan not in PLANS or plan == "free":
        await query.answer("❌ Неверный тариф", show_alert=True)
        return
    
    plan_info = PLANS[plan]
    
    if method == "stars":
        # Telegram Stars payment
        price_stars = plan_info.get("price_stars", 150)
        await context.bot.send_invoice(
            chat_id=chat_id,
            title=f"Nodkeys {plan_info['name']} — 1 месяц",
            description=f"Подписка {plan_info['name']} на 1 месяц. Все функции без ограничений.",
            payload=f"sub_{plan}_{user_id}",
            provider_token="",  # empty for Stars
            currency="XTR",
            prices=[LabeledPrice(label=f"{plan_info['name']} 1 мес", amount=price_stars)],
        )
    
    elif method == "yookassa":
        # YooKassa payment
        if not YOOKASSA_TOKEN:
            await query.answer("❌ ЮKassa не настроена", show_alert=True)
            return
        
        price_rub = plan_info.get("price_rub", 299)
        await context.bot.send_invoice(
            chat_id=chat_id,
            title=f"Nodkeys {plan_info['name']} — 1 месяц",
            description=f"Подписка {plan_info['name']} на 1 месяц.",
            payload=f"sub_{plan}_{user_id}",
            provider_token=YOOKASSA_TOKEN,
            currency="RUB",
            prices=[LabeledPrice(label=f"{plan_info['name']} 1 мес", amount=price_rub * 100)],  # kopecks
            need_email=True,
        )


async def precheckout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle pre-checkout query — approve payment."""
    query = update.pre_checkout_query
    # Always approve (can add validation here)
    await query.answer(ok=True)


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle successful payment — activate subscription."""
    payment = update.message.successful_payment
    payload = payment.invoice_payload  # sub_pro_123456
    
    parts = payload.split("_")
    if len(parts) < 3 or parts[0] != "sub":
        return
    
    plan = parts[1]
    user_id = int(parts[2])
    
    # Determine payment method
    if payment.currency == "XTR":
        method = "stars"
    else:
        method = "yookassa"
    
    # Activate subscription
    activate_subscription(user_id, plan, months=1, payment_method=method)
    
    plan_name = PLANS.get(plan, {}).get("name", plan)
    await update.message.reply_text(
        f"✅ Подписка **{plan_name}** активирована на 1 месяц!\n\n"
        f"Спасибо за поддержку! Все функции теперь доступны без ограничений.",
        parse_mode="Markdown"
    )
    logger.info("Subscription activated: user=%d plan=%s method=%s", user_id, plan, method)
