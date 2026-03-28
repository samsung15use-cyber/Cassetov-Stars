import telebot
import sqlite3
import time
import os
import logging
from telebot import types
from datetime import datetime, timedelta
import random
import string
import json
import threading
from functools import wraps

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = "8452691403:AAHNUQvOUduuFlqCrJn17Q_sFTR54v1a9to"
ADMIN_IDS = [1417003901]
DB_NAME = "gifts_bot.db"
bot = telebot.TeleBot(BOT_TOKEN)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Настройки реферальной системы
REFERRAL_BONUS = 5
REFERRAL_PERCENT = 5
DAILY_BONUS = 1

# Товары (подарки)
GIFTS = {
    "🎁 Подарок": 22,
    "🌹 Роза": 22,
    "🚀 Ракета": 47,
    "💐 Букет": 47,
    "🎂 Торт": 47,
    "🍾 Шампанское": 47,
    "💎 Алмаз": 97,
    "💍 Кольцо": 97,
    "🏆 Кубок": 97,
    "🎄 Ёлочка": 50,
    "🧸 Новогодний мишка": 50,
    "❤️ Сердце 14 февраля": 50,
    "🧸 Мишка 14 февраля": 50,
    "🐻 Мишка": 13,
    "💖 Сердце": 13
}

# ==================== КЛАСС ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ====================
class Database:
    def __init__(self, db_name):
        self.db_name = db_name
        self.local = threading.local()
        self._lock = threading.Lock()
        
    def get_connection(self):
        if not hasattr(self.local, 'connection') or self.local.connection is None:
            self.local.connection = sqlite3.connect(
                self.db_name, 
                check_same_thread=False,
                timeout=30
            )
            self.local.connection.row_factory = sqlite3.Row
            self.local.connection.execute("PRAGMA journal_mode=WAL")
            self.local.connection.execute("PRAGMA synchronous=NORMAL")
        return self.local.connection
    
    def execute(self, query, params=None):
        with self._lock:
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                conn.commit()
                return cursor
            except Exception as e:
                logger.error(f"Database error: {e}\nQuery: {query}")
                raise
    
    def close(self):
        if hasattr(self.local, 'connection') and self.local.connection:
            self.local.connection.close()
            self.local.connection = None

db = Database(DB_NAME)

# ==================== ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ====================
def init_database():
    """Создание оптимизированной структуры базы данных"""
    try:
        if os.path.exists(DB_NAME):
            backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            try:
                import shutil
                shutil.copy(DB_NAME, backup_name)
                logger.info(f"💾 Создан бэкап: {backup_name}")
            except:
                pass
            os.remove(DB_NAME)
            logger.info("🗑️ Старая база данных удалена")
        
        # Таблица пользователей
        db.execute('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            balance REAL DEFAULT 0,
            total_earned REAL DEFAULT 0,
            total_spent REAL DEFAULT 0,
            gifts_received INTEGER DEFAULT 0,
            referrer_id INTEGER DEFAULT NULL,
            referral_count INTEGER DEFAULT 0,
            referral_earnings REAL DEFAULT 0,
            registration_date TEXT,
            last_active TEXT,
            last_bonus_date TEXT,
            bonus_streak INTEGER DEFAULT 1,
            notifications INTEGER DEFAULT 1,
            is_banned INTEGER DEFAULT 0,
            language TEXT DEFAULT 'ru',
            invite_link TEXT UNIQUE,
            referral_code TEXT UNIQUE
        )
        ''')
        
        # Таблица спонсоров
        db.execute('''
        CREATE TABLE sponsors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            link TEXT NOT NULL,
            chat_id TEXT NOT NULL UNIQUE,
            date_added TEXT,
            is_active INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0
        )
        ''')
        
        # Таблица рефералов
        db.execute('''
        CREATE TABLE referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            referral_id INTEGER NOT NULL,
            date TEXT,
            earnings REAL DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
        ''')
        
        # Таблица транзакций
        db.execute('''
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            gift_name TEXT,
            transaction_type TEXT NOT NULL,
            description TEXT,
            date TEXT,
            status TEXT DEFAULT 'completed'
        )
        ''')
        
        # Таблица для рассылок
        db.execute('''
        CREATE TABLE temp_mailing (
            admin_id INTEGER PRIMARY KEY,
            text TEXT,
            created_at TEXT
        )
        ''')
        
        # Таблица для статистики
        db.execute('''
        CREATE TABLE statistics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            total_users INTEGER,
            new_users INTEGER,
            active_users INTEGER,
            total_purchases REAL,
            total_referral_earnings REAL,
            total_balance REAL
        )
        ''')
        
        # Создание индексов
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_referrer ON users(referrer_id)",
            "CREATE INDEX IF NOT EXISTS idx_users_registration ON users(registration_date)",
            "CREATE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)",
            "CREATE INDEX IF NOT EXISTS idx_referrals_user ON referrals(user_id)"
        ]
        
        for index in indexes:
            try:
                db.execute(index)
            except:
                pass
        
        logger.info("✅ База данных успешно инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

# ==================== ФУНКЦИИ РАБОТЫ С БАЗОЙ ДАННЫХ ====================
def get_user(user_id):
    """Получение пользователя из БД"""
    try:
        cursor = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Ошибка получения пользователя: {e}")
        return None

def update_user_activity(user_id):
    """Обновление активности пользователя"""
    try:
        db.execute("UPDATE users SET last_active = ? WHERE user_id = ?",
                  (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
    except Exception as e:
        logger.error(f"Ошибка обновления активности: {e}")

def generate_unique_code():
    """Генерация уникального реферального кода"""
    while True:
        code = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        cursor = db.execute("SELECT referral_code FROM users WHERE referral_code = ?", (code,))
        exists = cursor.fetchone()
        if not exists:
            return code

def register_user(message):
    """Регистрация нового пользователя"""
    try:
        user_id = message.from_user.id
        username = message.from_user.username or ""
        first_name = message.from_user.first_name or ""
        last_name = message.from_user.last_name or ""
        
        cursor = db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            referral_code = generate_unique_code()
            bot_username = bot.get_me().username
            invite_link = f"https://t.me/{bot_username}?start={referral_code}"
            
            referrer_id = None
            if message.text and message.text.startswith('/start '):
                ref_code = message.text[7:]
                cursor = db.execute("SELECT user_id FROM users WHERE referral_code = ?", (ref_code,))
                result = cursor.fetchone()
                if result:
                    referrer_id = result[0]
                    logger.info(f"👤 Новый пользователь пришел по реферальной ссылке от {referrer_id}")
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db.execute('''
            INSERT INTO users 
            (user_id, username, first_name, last_name, registration_date, 
             last_active, invite_link, referral_code, referrer_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name, now, now, 
                  invite_link, referral_code, referrer_id))
            
            if referrer_id:
                db.execute('''
                UPDATE users 
                SET balance = balance + ?, 
                    total_earned = total_earned + ?,
                    referral_count = referral_count + 1 
                WHERE user_id = ?
                ''', (REFERRAL_BONUS, REFERRAL_BONUS, referrer_id))
                
                db.execute('''
                INSERT INTO referrals (user_id, referral_id, date, earnings)
                VALUES (?, ?, ?, ?)
                ''', (referrer_id, user_id, now, REFERRAL_BONUS))
                
                try:
                    bot.send_message(
                        referrer_id,
                        f"🎉 У тебя новый реферал!\n\n"
                        f"👤 {first_name or username or 'Пользователь'}\n"
                        f"💰 Начислено: +{REFERRAL_BONUS} ⭐"
                    )
                except:
                    pass
                
                logger.info(f"✅ Реферал зарегистрирован: {user_id} приглашен {referrer_id}")
            
            logger.info(f"✅ Новый пользователь зарегистрирован: {user_id}")
        
        return user
    except Exception as e:
        logger.error(f"❌ Ошибка регистрации: {e}")
        return None

def get_sponsors():
    """Получение списка спонсоров"""
    try:
        cursor = db.execute("SELECT name, link, chat_id FROM sponsors WHERE is_active = 1 ORDER BY sort_order")
        sponsors = cursor.fetchall()
        return [{"name": s[0], "link": s[1], "chat_id": s[2]} for s in sponsors]
    except:
        return []

def add_sponsor(name, link, chat_id):
    """Добавление спонсора"""
    try:
        db.execute(
            "INSERT INTO sponsors (name, link, chat_id, date_added) VALUES (?, ?, ?, ?)",
            (name, link, chat_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        return True
    except:
        return False

def delete_sponsor(chat_id):
    """Удаление спонсора"""
    try:
        db.execute("DELETE FROM sponsors WHERE chat_id = ?", (chat_id,))
        return True
    except:
        return False

def check_subscription(user_id):
    """Проверка подписки на спонсоров"""
    sponsors = get_sponsors()
    if not sponsors:
        return True, []
    
    not_subscribed = []
    try:
        for sponsor in sponsors:
            try:
                chat_id = sponsor['chat_id']
                if str(chat_id).startswith('@'):
                    chat = bot.get_chat(chat_id)
                    chat_id = chat.id
                
                member = bot.get_chat_member(chat_id, user_id)
                if member.status in ['left', 'kicked']:
                    not_subscribed.append(sponsor)
            except Exception as e:
                logger.error(f"Ошибка проверки подписки на {sponsor['name']}: {e}")
                not_subscribed.append(sponsor)
        
        return len(not_subscribed) == 0, not_subscribed
    except Exception as e:
        logger.error(f"Общая ошибка проверки подписки: {e}")
        return False, sponsors

def check_admin_status(user_id):
    """Проверка прав администратора"""
    return user_id in ADMIN_IDS

def purchase_gift(user_id, gift_name):
    """Покупка подарка"""
    try:
        price = GIFTS.get(gift_name)
        if not price:
            return False, "Подарок не найден"
        
        cursor = db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()
        
        if not user or user[0] < price:
            return False, "Недостаточно звезд"
        
        # Списываем средства
        db.execute("UPDATE users SET balance = balance - ?, total_spent = total_spent + ? WHERE user_id = ?",
                  (price, price, user_id))
        
        # Записываем транзакцию
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db.execute('''
        INSERT INTO transactions (user_id, amount, gift_name, transaction_type, description, date)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, -price, gift_name, "purchase", f"Покупка {gift_name}", now))
        
        # Начисляем комиссию рефереру
        cursor = db.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
        referrer = cursor.fetchone()
        
        if referrer and referrer[0]:
            referrer_earnings = price * REFERRAL_PERCENT / 100
            db.execute('''
            UPDATE users 
            SET balance = balance + ?, total_earned = total_earned + ?, 
                referral_earnings = referral_earnings + ? 
            WHERE user_id = ?
            ''', (referrer_earnings, referrer_earnings, referrer_earnings, referrer[0]))
            
            db.execute('''
            INSERT INTO transactions (user_id, amount, transaction_type, description, date)
            VALUES (?, ?, ?, ?, ?)
            ''', (referrer[0], referrer_earnings, "referral_commission", 
                  f"{REFERRAL_PERCENT}% от покупки реферала", now))
            
            db.execute('''
            UPDATE referrals 
            SET earnings = earnings + ? 
            WHERE user_id = ? AND referral_id = ?
            ''', (referrer_earnings, referrer[0], user_id))
        
        return True, price
    except Exception as e:
        logger.error(f"Ошибка покупки: {e}")
        return False, str(e)

# ==================== ИНЛАЙН КЛАВИАТУРЫ ====================
def main_menu_keyboard(user_id):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("🎁 Подарки", callback_data="menu_gifts"),
        types.InlineKeyboardButton("⭐️ Заработать", callback_data="menu_earn"),
        types.InlineKeyboardButton("👤 Профиль", callback_data="menu_profile"),
    ]
    
    if check_admin_status(user_id):
        buttons.append(types.InlineKeyboardButton("⚙️ Админ панель", callback_data="menu_admin"))
    
    keyboard.add(*buttons)
    return keyboard

def admin_menu_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("📊 Статистика", callback_data="admin_stats"),
        types.InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
        types.InlineKeyboardButton("📢 Спонсоры", callback_data="admin_sponsors"),
        types.InlineKeyboardButton("📨 Рассылка", callback_data="admin_mailing"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")
    ]
    keyboard.add(*buttons)
    return keyboard

def subscription_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    sponsors = get_sponsors()
    for sponsor in sponsors:
        keyboard.add(types.InlineKeyboardButton(
            text=f"📢 {sponsor['name']}", 
            url=sponsor['link']
        ))
    keyboard.add(types.InlineKeyboardButton(
        text="✅ Я подписался", 
        callback_data="check_subscription"
    ))
    return keyboard

def gifts_keyboard(user_balance):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    row = []
    for i, (gift, price) in enumerate(GIFTS.items(), 1):
        button = types.InlineKeyboardButton(
            text=f"{gift} - {price} ⭐", 
            callback_data=f"buy_{gift}"
        )
        row.append(button)
        if i % 2 == 0:
            keyboard.row(*row)
            row = []
    if row:
        keyboard.row(*row)
    keyboard.row(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    return keyboard

def earn_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("📤 Поделиться ссылкой", callback_data="share_link"),
        types.InlineKeyboardButton("👥 Мои рефералы", callback_data="my_referrals"),
        types.InlineKeyboardButton("🎁 Ежедневный бонус", callback_data="daily_bonus"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")
    ]
    keyboard.add(*buttons)
    return keyboard

def back_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    return keyboard

def sponsors_management_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton("➕ Добавить", callback_data="sponsor_add"),
        types.InlineKeyboardButton("❌ Удалить", callback_data="sponsor_del"),
        types.InlineKeyboardButton("🗑️ Очистить", callback_data="sponsor_clear"),
        types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")
    ]
    keyboard.add(*buttons)
    return keyboard

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
@bot.message_handler(commands=['start'])
def start_command(message):
    try:
        user_id = message.from_user.id
        register_user(message)
        update_user_activity(user_id)
        
        is_subscribed, not_subscribed = check_subscription(user_id)
        
        if not is_subscribed:
            keyboard = types.InlineKeyboardMarkup(row_width=1)
            for sponsor in not_subscribed:
                keyboard.add(types.InlineKeyboardButton(
                    text=f"📢 {sponsor['name']}", 
                    url=sponsor['link']
                ))
            keyboard.add(types.InlineKeyboardButton(
                text="✅ Я подписался", 
                callback_data="check_subscription"
            ))
            
            bot.send_message(
                user_id,
                "🚫 **Для использования бота необходимо подписаться на спонсоров:**\n\n"
                "Подпишись на каналы ниже и нажми кнопку 'Я подписался'",
                parse_mode="Markdown",
                reply_markup=keyboard
            )
            return
        
        welcome_text = (
            "🎁 Добро пожаловать в Cassetov Stars!\n\n"
            "✨ Здесь ты можешь зарабатывать звезды\n"
            "👥 Приглашай друзей и получай бонусы\n"
            f"💫 Зарабатывай {REFERRAL_PERCENT}% от трат рефералов\n"
            f"🎁 Бонус за друга: {REFERRAL_BONUS} ⭐\n"
            f"🎁 Ежедневный бонус: {DAILY_BONUS} ⭐"
        )
        
        bot.send_message(user_id, welcome_text, reply_markup=main_menu_keyboard(user_id))
    except Exception as e:
        logger.error(f"Ошибка в start: {e}")

@bot.message_handler(commands=['ref'])
def ref_command(message):
    """Показать реферальную ссылку"""
    try:
        user_id = message.from_user.id
        update_user_activity(user_id)
        
        is_subscribed, not_subscribed = check_subscription(user_id)
        
        if not is_subscribed:
            keyboard = types.InlineKeyboardMarkup(row_width=1)
            for sponsor in not_subscribed:
                keyboard.add(types.InlineKeyboardButton(
                    text=f"📢 {sponsor['name']}", 
                    url=sponsor['link']
                ))
            keyboard.add(types.InlineKeyboardButton(
                text="✅ Я подписался", 
                callback_data="check_subscription"
            ))
            
            bot.send_message(
                user_id,
                "🚫 **Для использования бота необходимо подписаться на спонсоров:**\n\n"
                "Подпишись на каналы ниже и нажми кнопку 'Я подписался'",
                parse_mode="Markdown",
                reply_markup=keyboard
            )
            return
            
        user = get_user(message.from_user.id)
        if user:
            bot.send_message(
                message.chat.id,
                f"🔗 **Твоя реферальная ссылка:**\n`{user['invite_link']}`\n\n"
                f"📊 **Статистика:**\n"
                f"• Приглашено: {user['referral_count']} чел.\n"
                f"• Заработано: {user['referral_earnings']} ⭐",
                parse_mode="Markdown"
            )
    except Exception as e:
        logger.error(f"Ошибка в ref: {e}")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    try:
        user_id = call.from_user.id
        user = get_user(user_id)
        update_user_activity(user_id)
        
        # Проверка подписки
        if call.data != "check_subscription" and not call.data.startswith("admin_") and call.data != "daily_bonus":
            is_subscribed, not_subscribed = check_subscription(user_id)
            if not is_subscribed:
                bot.answer_callback_query(call.id, "❌ Сначала подпишись на спонсоров!", show_alert=True)
                
                keyboard = types.InlineKeyboardMarkup(row_width=1)
                for sponsor in not_subscribed:
                    keyboard.add(types.InlineKeyboardButton(
                        text=f"📢 {sponsor['name']}", 
                        url=sponsor['link']
                    ))
                keyboard.add(types.InlineKeyboardButton(
                    text="✅ Я подписался", 
                    callback_data="check_subscription"
                ))
                
                try:
                    bot.edit_message_text(
                        "🚫 **Для использования бота необходимо подписаться на спонсоров:**\n\n"
                        "Подпишись на каналы ниже и нажми кнопку 'Я подписался'",
                        user_id,
                        call.message.message_id,
                        parse_mode="Markdown",
                        reply_markup=keyboard
                    )
                except:
                    pass
                return
        
        # ===== ГЛАВНОЕ МЕНЮ =====
        if call.data == "back_to_main":
            bot.edit_message_text(
                "Главное меню:",
                user_id,
                call.message.message_id,
                reply_markup=main_menu_keyboard(user_id)
            )
        
        elif call.data == "back_to_admin":
            if check_admin_status(user_id):
                bot.edit_message_text(
                    "⚙️ Админ панель:",
                    user_id,
                    call.message.message_id,
                    reply_markup=admin_menu_keyboard()
                )
        
        # ===== МЕНЮ ПОЛЬЗОВАТЕЛЯ =====
        elif call.data == "menu_gifts":
            if user:
                text = f"🎁 **Доступные подарки**\n💰 Твой баланс: {user['balance']} ⭐\n\nВыбери подарок для покупки:"
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=gifts_keyboard(user['balance'])
                )
        
        elif call.data == "menu_earn":
            if user:
                ref_link = user['invite_link']
                text = (
                    "💰 **Как заработать звезды?**\n\n"
                    "👥 **Реферальная система**\n"
                    f"• За каждого друга: +{REFERRAL_BONUS} ⭐\n"
                    f"• {REFERRAL_PERCENT}% от всех трат друзей\n\n"
                    "🎁 **Ежедневный бонус**\n"
                    f"• Каждый день: +{DAILY_BONUS} ⭐\n"
                    "• За серию посещений бонус увеличивается!\n\n"
                    "🔗 **Твоя реферальная ссылка:**\n"
                    f"`{ref_link}`\n\n"
                    "📊 **Твоя статистика:**\n"
                    f"• Приглашено друзей: {user['referral_count']} чел.\n"
                    f"• Заработано с рефералов: {user['referral_earnings']} ⭐\n"
                    f"• Всего заработано: {user['total_earned']} ⭐"
                )
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=earn_keyboard()
                )
        
        elif call.data == "menu_profile":
            if user:
                cursor = db.execute("SELECT COUNT(*) FROM transactions WHERE user_id = ? AND transaction_type = 'purchase'", (user_id,))
                gifts_bought = cursor.fetchone()[0]
                
                text = (
                    f"👤 **Профиль пользователя**\n\n"
                    f"🆔 ID: `{user_id}`\n"
                    f"📅 Регистрация: {user['registration_date']}\n"
                    f"📊 Серия бонусов: {user['bonus_streak']} дней\n\n"
                    f"💰 **Баланс:** {user['balance']} ⭐\n"
                    f"🎁 Куплено подарков: {gifts_bought}\n"
                    f"👥 Приглашено друзей: {user['referral_count']}\n"
                    f"💫 Заработано всего: {user['total_earned']} ⭐\n"
                    f"💸 Потрачено всего: {user['total_spent']} ⭐"
                )
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=back_keyboard()
                )
        
        # ===== РЕФЕРАЛЬНЫЕ ФУНКЦИИ =====
        elif call.data == "share_link":
            if user:
                ref_link = user['invite_link']
                
                share_keyboard = types.InlineKeyboardMarkup()
                share_keyboard.add(types.InlineKeyboardButton(
                    text="📤 Поделиться ссылкой",
                    url=f"https://t.me/share/url?url={ref_link}&text=%F0%9F%8E%81%20%D0%9F%D0%BE%D0%BB%D1%83%D1%87%D0%B0%D0%B9%20%D0%BF%D0%BE%D0%B4%D0%B0%D1%80%D0%BA%D0%B8%20%D0%B7%D0%B0%20%D0%B7%D0%B2%D0%B5%D0%B7%D0%B4%D1%8B%21%20%D0%9F%D1%80%D0%B8%D1%81%D0%BE%D0%B5%D0%B4%D0%B8%D0%BD%D1%8F%D0%B9%D1%81%D1%8F%20%D0%BF%D0%BE%20%D0%BC%D0%BE%D0%B5%D0%B9%20%D1%81%D1%81%D1%8B%D0%BB%D0%BA%D0%B5%3A"
                ))
                share_keyboard.add(types.InlineKeyboardButton(
                    text="🔙 Назад к заработку",
                    callback_data="menu_earn"
                ))
                
                bot.edit_message_text(
                    f"🔗 **Твоя реферальная ссылка:**\n\n`{ref_link}`\n\n📤 Нажми кнопку ниже, чтобы поделиться ссылкой с друзьями!",
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=share_keyboard
                )
        
        elif call.data == "my_referrals":
            cursor = db.execute('''
            SELECT u.username, u.first_name, r.date, r.earnings 
            FROM referrals r
            JOIN users u ON r.referral_id = u.user_id
            WHERE r.user_id = ?
            ORDER BY r.date DESC
            ''', (user_id,))
            referrals = cursor.fetchall()
            
            if not referrals:
                text = "👥 У тебя пока нет рефералов.\nПриглашай друзей по своей ссылке и получай бонусы!"
            else:
                text = "👥 **Твои рефералы:**\n\n"
                for i, ref in enumerate(referrals, 1):
                    name = ref[1] or f"@{ref[0]}" if ref[0] else "Пользователь"
                    date = ref[2][:10] if ref[2] else "неизвестно"
                    earnings = ref[3]
                    text += f"{i}. {name}\n   📅 {date} | 💰 {earnings} ⭐\n\n"
            
            bot.edit_message_text(
                text,
                user_id,
                call.message.message_id,
                parse_mode="Markdown",
                reply_markup=back_keyboard()
            )
        
        # ===== ЕЖЕДНЕВНЫЙ БОНУС =====
        elif call.data == "daily_bonus":
            if user:
                cursor = db.execute("SELECT last_bonus_date FROM users WHERE user_id = ?", (user_id,))
                result = cursor.fetchone()
                last_bonus = result[0] if result else None
                
                today = datetime.now().strftime("%Y-%m-%d")
                
                if last_bonus == today:
                    bot.answer_callback_query(call.id, "❌ Ты уже получил сегодняшний бонус!", show_alert=True)
                    return
                
                # Начисляем бонус
                streak = user['bonus_streak']
                if last_bonus:
                    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                    if last_bonus == yesterday:
                        streak += 1
                    else:
                        streak = 1
                else:
                    streak = 1
                
                bonus = DAILY_BONUS + (streak - 1) * 0.5
                bonus = min(bonus, DAILY_BONUS * 3)
                
                db.execute('''
                UPDATE users 
                SET balance = balance + ?, total_earned = total_earned + ?,
                    last_bonus_date = ?, bonus_streak = ?
                WHERE user_id = ?
                ''', (bonus, bonus, today, streak, user_id))
                
                db.execute('''
                INSERT INTO transactions (user_id, amount, transaction_type, description, date)
                VALUES (?, ?, ?, ?, ?)
                ''', (user_id, bonus, "daily_bonus", f"Ежедневный бонус (серия: {streak})", 
                      datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                
                bot.answer_callback_query(call.id, f"✅ Получено {bonus} ⭐! Серия: {streak} дней", show_alert=True)
                
                # Обновляем сообщение
                user = get_user(user_id)
                ref_link = user['invite_link']
                text = (
                    "💰 **Как заработать звезды?**\n\n"
                    "👥 **Реферальная система**\n"
                    f"• За каждого друга: +{REFERRAL_BONUS} ⭐\n"
                    f"• {REFERRAL_PERCENT}% от всех трат друзей\n\n"
                    "🎁 **Ежедневный бонус**\n"
                    f"• Каждый день: +{DAILY_BONUS} ⭐\n"
                    "• За серию посещений бонус увеличивается!\n\n"
                    "🔗 **Твоя реферальная ссылка:**\n"
                    f"`{ref_link}`\n\n"
                    "📊 **Твоя статистика:**\n"
                    f"• Приглашено друзей: {user['referral_count']} чел.\n"
                    f"• Заработано с рефералов: {user['referral_earnings']} ⭐\n"
                    f"• Всего заработано: {user['total_earned']} ⭐"
                )
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=earn_keyboard()
                )
        
        # ===== ПОКУПКА ПОДАРКОВ =====
        elif call.data.startswith('buy_'):
            gift_name = call.data[4:]
            price = GIFTS.get(gift_name)
            
            if not price or not user:
                return
            
            if user['balance'] < price:
                bot.answer_callback_query(call.id, f"❌ Недостаточно звезд! Нужно: {price} ⭐", show_alert=True)
                return
            
            success, result = purchase_gift(user_id, gift_name)
            
            if success:
                bot.answer_callback_query(call.id, f"✅ Покупка совершена!", show_alert=False)
                
                delivery_text = (
                    f"🎁 **Покупка успешно оформлена!**\n\n"
                    f"Ты купил: {gift_name}\n"
                    f"Цена: {price} ⭐\n"
                    f"Остаток на балансе: {user['balance'] - price} ⭐\n\n"
                    f"⏱️ **Подарок будет доставлен в течение нескольких минут!**\n"
                    f"Ожидай, скоро получишь свой подарок!"
                )
                
                bot.send_message(
                    user_id,
                    delivery_text,
                    parse_mode="Markdown"
                )
                
                user = get_user(user_id)
                bot.edit_message_text(
                    f"🎁 **Доступные подарки**\n💰 Твой баланс: {user['balance']} ⭐\n\nВыбери подарок для покупки:",
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=gifts_keyboard(user['balance'])
                )
            else:
                bot.answer_callback_query(call.id, f"❌ Ошибка: {result}", show_alert=True)
        
        # ===== ПРОВЕРКА ПОДПИСКИ =====
        elif call.data == "check_subscription":
            is_subscribed, not_subscribed = check_subscription(user_id)
            
            if is_subscribed:
                bot.edit_message_text(
                    "✅ Спасибо за подписку! Добро пожаловать!",
                    user_id,
                    call.message.message_id
                )
                bot.send_message(user_id, "🎁 Cassetov Stars:", reply_markup=main_menu_keyboard(user_id))
            else:
                keyboard = types.InlineKeyboardMarkup(row_width=1)
                for sponsor in not_subscribed:
                    keyboard.add(types.InlineKeyboardButton(
                        text=f"📢 {sponsor['name']}", 
                        url=sponsor['link']
                    ))
                keyboard.add(types.InlineKeyboardButton(
                    text="✅ Я подписался", 
                    callback_data="check_subscription"
                ))
                
                bot.answer_callback_query(call.id, "❌ Вы не подписались на всех спонсоров!", show_alert=True)
                bot.edit_message_text(
                    "🚫 **Для использования бота необходимо подписаться на спонсоров:**\n\n"
                    "Подпишись на каналы ниже и нажми кнопку 'Я подписался'",
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=keyboard
                )
        
        # ===== АДМИН МЕНЮ =====
        elif call.data == "menu_admin":
            if check_admin_status(user_id):
                bot.edit_message_text(
                    "⚙️ Админ панель:",
                    user_id,
                    call.message.message_id,
                    reply_markup=admin_menu_keyboard()
                )
            else:
                bot.answer_callback_query(call.id, "❌ У вас нет прав администратора!", show_alert=True)
        
        elif call.data == "admin_stats":
            if check_admin_status(user_id):
                cursor = db.execute("SELECT COUNT(*) FROM users")
                total_users = cursor.fetchone()[0]
                
                cursor = db.execute("SELECT COUNT(*) FROM users WHERE DATE(registration_date) = DATE('now')")
                new_today = cursor.fetchone()[0]
                
                cursor = db.execute("SELECT SUM(amount) FROM transactions WHERE transaction_type = 'purchase'")
                total_purchases = cursor.fetchone()[0] or 0
                
                cursor = db.execute("SELECT SUM(referral_earnings) FROM users")
                total_referral_paid = cursor.fetchone()[0] or 0
                
                cursor = db.execute("SELECT SUM(balance) FROM users")
                total_balance = cursor.fetchone()[0] or 0
                
                cursor = db.execute("SELECT COUNT(*) FROM users WHERE last_active > datetime('now', '-1 day')")
                active_today = cursor.fetchone()[0]
                
                text = (
                    "📊 **Статистика бота**\n\n"
                    f"👥 Всего пользователей: {total_users}\n"
                    f"📅 Новых сегодня: {new_today}\n"
                    f"🟢 Активных за 24ч: {active_today}\n"
                    f"💰 Всего покупок: {abs(total_purchases)} ⭐\n"
                    f"💫 Выплачено рефералам: {total_referral_paid} ⭐\n"
                    f"💎 Общий баланс: {total_balance} ⭐"
                )
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=back_keyboard()
                )
        
        elif call.data == "admin_users":
            if check_admin_status(user_id):
                cursor = db.execute("SELECT user_id, username, first_name, balance, referral_count, registration_date FROM users ORDER BY registration_date DESC LIMIT 10")
                users = cursor.fetchall()
                
                text = "👥 **Последние 10 пользователей:**\n\n"
                for u in users:
                    name = u[2] or f"@{u[1]}" if u[1] else f"ID: {u[0]}"
                    text += f"• {name}\n"
                    text += f"  ID: `{u[0]}` | Баланс: {u[3]} ⭐ | Рефералов: {u[4]}\n"
                    text += f"  Дата: {u[5][:10]}\n\n"
                
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=back_keyboard()
                )
        
        elif call.data == "admin_sponsors":
            if check_admin_status(user_id):
                sponsors = get_sponsors()
                text = "📢 **Управление спонсорами**\n\n"
                
                if sponsors:
                    text += "**Текущие спонсоры:**\n"
                    for i, s in enumerate(sponsors, 1):
                        text += f"{i}. {s['name']} - {s['chat_id']}\n"
                else:
                    text += "Спонсоры отсутствуют\n"
                
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=sponsors_management_keyboard()
                )
        
        elif call.data == "sponsor_add":
            if check_admin_status(user_id):
                bot.edit_message_text(
                    "📝 Отправьте мне данные спонсора в формате:\n`Название | ссылка | @канал`\n\nНапример:\n`Мой канал | https://t.me/mychannel | @mychannel`",
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown"
                )
                bot.register_next_step_handler_by_chat_id(user_id, process_add_sponsor)
        
        elif call.data == "sponsor_del":
            if check_admin_status(user_id):
                sponsors = get_sponsors()
                if not sponsors:
                    bot.answer_callback_query(call.id, "❌ Нет спонсоров для удаления", show_alert=True)
                    return
                
                text = "❌ Выберите спонсора для удаления:\n\n"
                keyboard = types.InlineKeyboardMarkup(row_width=1)
                for s in sponsors:
                    keyboard.add(types.InlineKeyboardButton(
                        text=f"{s['name']} - {s['chat_id']}",
                        callback_data=f"del_sponsor_{s['chat_id']}"
                    ))
                keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_sponsors"))
                
                bot.edit_message_text(
                    text,
                    user_id,
                    call.message.message_id,
                    reply_markup=keyboard
                )
        
        elif call.data.startswith("del_sponsor_"):
            if check_admin_status(user_id):
                chat_id = call.data.replace("del_sponsor_", "")
                if delete_sponsor(chat_id):
                    bot.answer_callback_query(call.id, "✅ Спонсор удален!", show_alert=True)
                    sponsors = get_sponsors()
                    text = "📢 **Управление спонсорами**\n\n"
                    if sponsors:
                        text += "**Текущие спонсоры:**\n"
                        for i, s in enumerate(sponsors, 1):
                            text += f"{i}. {s['name']} - {s['chat_id']}\n"
                    else:
                        text += "Спонсоры отсутствуют\n"
                    
                    bot.edit_message_text(
                        text,
                        user_id,
                        call.message.message_id,
                        parse_mode="Markdown",
                        reply_markup=sponsors_management_keyboard()
                    )
        
        elif call.data == "sponsor_clear":
            if check_admin_status(user_id):
                db.execute("DELETE FROM sponsors")
                bot.answer_callback_query(call.id, "✅ Все спонсоры удалены!", show_alert=True)
                
                bot.edit_message_text(
                    "📢 **Управление спонсорами**\n\nСпонсоры отсутствуют",
                    user_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=sponsors_management_keyboard()
                )
        
        elif call.data == "admin_mailing":
            if check_admin_status(user_id):
                bot.edit_message_text(
                    "📨 Введите текст для рассылки:",
                    user_id,
                    call.message.message_id
                )
                bot.register_next_step_handler_by_chat_id(user_id, process_mailing)
    
    except Exception as e:
        logger.error(f"❌ Ошибка в callback: {e}")

def process_add_sponsor(message):
    try:
        user_id = message.from_user.id
        if not check_admin_status(user_id):
            return
        
        text = message.text
        parts = text.split('|')
        
        if len(parts) < 3:
            bot.send_message(
                user_id,
                "❌ Неверный формат. Используйте: Название | ссылка | @канал",
                reply_markup=admin_menu_keyboard()
            )
            return
        
        name = parts[0].strip()
        link = parts[1].strip()
        chat_id = parts[2].strip()
        
        if add_sponsor(name, link, chat_id):
            bot.send_message(user_id, f"✅ Спонсор {name} добавлен!", reply_markup=admin_menu_keyboard())
        else:
            bot.send_message(user_id, "❌ Ошибка: спонсор с таким @каналом уже существует", reply_markup=admin_menu_keyboard())
    except Exception as e:
        bot.send_message(user_id, f"❌ Ошибка: {e}", reply_markup=admin_menu_keyboard())

def process_mailing(message):
    try:
        admin_id = message.from_user.id
        text = message.text
        
        db.execute("INSERT OR REPLACE INTO temp_mailing (admin_id, text, created_at) VALUES (?, ?, ?)", 
                  (admin_id, text, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        
        cursor = db.execute("SELECT user_id FROM users WHERE notifications = 1")
        users = cursor.fetchall()
        
        bot.send_message(admin_id, f"📨 Рассылка началась... Всего пользователей: {len(users)}")
        
        success = 0
        failed = 0
        
        for user in users:
            try:
                bot.send_message(user[0], text)
                success += 1
                time.sleep(0.05)
            except:
                failed += 1
        
        bot.send_message(
            admin_id,
            f"✅ Рассылка завершена!\n\n📊 Успешно: {success}\n❌ Ошибок: {failed}",
            reply_markup=admin_menu_keyboard()
        )
    except Exception as e:
        bot.send_message(admin_id, f"❌ Ошибка: {e}", reply_markup=admin_menu_keyboard())

def back_keyboard():
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    return keyboard

# ==================== ЗАПУСК БОТА ====================
if __name__ == "__main__":
    logger.info("🚀 Бот запускается...")
    init_database()
    
    try:
        bot_info = bot.get_me()
        logger.info(f"✅ Бот @{bot_info.username} готов к работе!")
        logger.info(f"👑 Админы: {ADMIN_IDS}")
        logger.info("🔄 Нажмите Ctrl+C для остановки")
        
        while True:
            try:
                bot.polling(none_stop=True, interval=1, timeout=30)
            except Exception as e:
                logger.error(f"❌ Ошибка в polling: {e}")
                time.sleep(5)
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        db.close()
        logger.info("🔌 Соединение с БД закрыто")