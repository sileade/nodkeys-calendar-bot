# Nodkeys Bot v10.0

Nodkeys Bot — это All-in-One AI-ассистент в Telegram с E2E шифрованием, который заменяет 5 ключевых приложений для продуктивности и жизни: Calendar, Todoist, Notion, Trello и Zenmoney.

## 🌟 Ключевые возможности

### Основные модули ("5 в 1")

1. **Умный календарь (Apple/Google Calendar)**
   - Полная синхронизация с iCloud/CalDAV
   - Управление встречами через естественный язык
   - Утренний брифинг и еженедельное ревью

2. **Задачи и привычки (Todoist-killer)**
   - 4 списка задач (сегодня, неделя, месяц, когда-нибудь)
   - Трекер привычек с геймификацией и статистикой
   - Интеграция с Apple Reminders

3. **Заметки и база знаний (Notion-killer)**
   - Сохранение идей, ссылок, статей
   - Личный дневник с хронографией
   - Интеграция с Apple Notes

4. **Проекты и Канбан (Trello-killer)**
   - Создание проектов и декомпозиция задач
   - Визуальная канбан-доска (To Do → In Progress → Done)
   - Управление приоритетами и прогрессом прямо в Telegram

5. **Финансовый трекер (Zenmoney-killer)**
   - Учет доходов и расходов по категориям
   - Установка месячных бюджетов с алертами
   - Генерация графиков (pie/bar) и финансовых отчетов

6. **Библиотека и Аудиокниги (Kindle/Bookmate)**
   - Поиск текстовых книг (Flibusta/Jackett)
   - Отправка EPUB прямо на Kindle
   - Поиск аудиокниг на RuTracker
   - Автоматическое скачивание и загрузка в S3 (параллельная загрузка, 8 потоков)
   - Встроенный аудиоплеер прямо в чате Telegram (Play/Prev/Next)
   - Автоматическая подгрузка следующей главы при нажатии Play
   - Веб-плеер (Mini App) с полным списком файлов
   - Кэширование на S3 для мгновенного повторного доступа

### Новое в v10.0

7. **🔐 E2E шифрование (Zero-Knowledge)**
   - AES-256-GCM шифрование всех пользовательских данных
   - Ключ = PBKDF2-SHA256(user_id + APP_ENCRYPTION_SECRET, 150K итераций)
   - Никто (включая разработчика) не может прочитать данные пользователя
   - Нет промптов для пароля — шифрование прозрачно

8. **💳 Подписка (Telegram Stars + YooKassa)**
   - Free / Pro (299₽/150 Stars) / Pro+ (499₽/250 Stars)
   - Telegram Stars — работает в России через SberPay
   - YooKassa — прямые карточные платежи
   - Автоматическая активация после оплаты

9. **⚙️ Onboarding и настройки**
   - Пошаговая настройка интеграций (календарь, Kindle, заметки)
   - Управление часовым поясом и предпочтениями
   - Все данные шифруются при сохранении

10. **📱 Быстрые команды (iOS Shortcuts / Android)**
    - REST API для iOS Shortcuts и Android Tasker
    - 8 готовых команд: задачи, заметки, Kindle, книги, финансы, напоминания, дневник
    - Персональный API-токен для каждого пользователя
    - Инструкции по настройке для iOS и Android

11. **🤖 Свой бот (Pro+)**
    - Подключение собственного Telegram бота
    - Бот работает от имени пользователя
    - Свой username и аватар
    - Полный контроль

## 🚀 Команды

| Команда | Описание |
|---------|----------|
| `/start` | Приветствие и информация |
| `/settings` | Настройки и интеграции |
| `/subscribe` | Управление подпиской |
| `/shortcuts` | Быстрые команды для телефона |
| `/today` | События на сегодня |
| `/week` | События на неделю |
| `/habits` | Трекер привычек |
| `/remind` | Напоминания |
| `/projects` | Канбан-доска проектов |
| `/finance` | Финансовый дашборд |
| `/book` | Поиск книги |
| `/help` | Справка |

## 🔌 API Endpoints

| Endpoint | Method | Описание |
|----------|--------|----------|
| `/health` | GET | Статус бота |
| `/api/shortcuts/list` | GET | Список доступных быстрых команд |
| `/api/shortcut` | POST | Выполнить команду (Bearer auth) |
| `/audiobook/player` | GET | Веб-плеер аудиокниг |
| `/audiobook/api/files` | GET | API файлов аудиокниги |

## 🛠 Установка и запуск

### Docker (рекомендуется)

```bash
git clone https://github.com/sileade/nodkeys-calendar-bot.git
cd nodkeys-calendar-bot
cp .env.example .env
# Заполните .env
docker compose up -d --build
```

### Локально

```bash
pip install -r requirements.txt
python bot.py
```

### Переменные окружения

Основные:
- `TELEGRAM_BOT_TOKEN` — токен бота от @BotFather
- `TELEGRAM_CHAT_ID` — ID чата
- `CLAUDE_API_KEY` — ключ Anthropic API
- `APP_ENCRYPTION_SECRET` — секрет для E2E шифрования (обязателен!)

Опциональные:
- `YOOKASSA_PROVIDER_TOKEN` — токен YooKassa для карточных платежей
- `ICLOUD_USERNAME` / `ICLOUD_PASSWORD` — CalDAV
- `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` — хранилище аудиокниг
- `QBITTORRENT_URL` — для скачивания торрентов

Полный список — в `.env.example`.

## 📦 Зависимости

- `python-telegram-bot` — Telegram Bot API
- `anthropic` — Claude AI
- `cryptography` — AES-256-GCM, PBKDF2
- `httpx` — HTTP клиент
- `boto3` — S3 storage
- `caldav` — CalDAV синхронизация
- `beautifulsoup4` — парсинг
- `matplotlib` — графики
- `calibre` — конвертация книг (в Docker)
- `ffmpeg` — обработка аудио (в Docker)

## 📁 Структура проекта

```
├── bot.py              # Основной файл бота (все handlers)
├── crypto.py           # E2E шифрование (AES-256-GCM, PBKDF2)
├── user_store.py       # Профили, настройки, подписки
├── subscription.py     # Платежи (Stars + YooKassa)
├── onboarding.py       # Onboarding и /settings
├── shortcuts_api.py    # REST API для быстрых команд
├── user_bots.py        # Подключение своих ботов (Pro+)
├── kindle_handler.py   # Kindle: конвертация и отправка
├── ical_proxy.py       # iCal прокси для виджетов
├── audiobook_player.html # Веб-плеер аудиокниг
├── Dockerfile          # Docker образ
├── requirements.txt    # Python зависимости
└── .env.example        # Шаблон переменных окружения
```

## Changelog

### v10.0 (2026-05-03)
- **E2E шифрование**: AES-256-GCM, PBKDF2 key derivation, zero-knowledge
- **Подписка**: Telegram Stars + YooKassa, 3 тарифа (Free/Pro/Pro+)
- **Onboarding**: /settings, пошаговая настройка интеграций
- **Shortcuts API**: REST API для iOS Shortcuts и Android Tasker
- **Свой бот**: Pro+ пользователи могут подключить свой Telegram бот
- **Новые модули**: crypto.py, user_store.py, subscription.py, onboarding.py, shortcuts_api.py, user_bots.py

### v9.15 (2026-05-03)
- Кнопка "Скачать одним файлом" (merge аудио через ffmpeg)
- Кнопка "Следующая глава" под каждым аудиосообщением

### v9.11 (2026-05-03)
- Auto-preload: при нажатии Play отправляется текущая + следующая глава
- Кнопки Play/Prev/Next под сообщением с аудиокнигой

### v9.10 (2026-05-03)
- Inline кнопки управления плеером (Play/Prev/Next) прямо в чате
- Веб-плеер через URL-кнопку (вместо WebApp)

### v9.8 (2026-05-02)
- Параллельная загрузка на S3 (8 потоков, ThreadPoolExecutor)
- Ускорение загрузки в ~8 раз

### v9.6 (2026-05-02)
- Логирование pipeline аудиокниг
- Exception handling с fallback

### v9.0 (2026-05-01)
- Проекты и Канбан-доска
- Финансовый трекер с графиками
- Трекер привычек

## License

MIT
