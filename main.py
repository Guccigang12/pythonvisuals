import asyncio
import logging
import uuid
import os
import re
import aiohttp
import asyncpg
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import uvicorn

# --- НАСТРОЙКИ ---
BOT_TOKEN = "8826829961:AAGVqWOzNVoP-vmwDoVcIHRexQSs6RsO6mI"
ADMIN_ID = 1186053117

DATABASE_URL = os.environ.get("DATABASE_URL")
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:8000")
WS_URL = RENDER_URL.replace("http://", "ws://").replace("https://", "wss://")

# Инициализация
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

db_pool = None
db_initialized = False

# --- THREAD-SAFE DICTIONARIES ---
class AsyncSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = asyncio.Lock()

    async def get(self, key, default=None):
        async with self._lock:
            return self._dict.get(key, default)

    async def set(self, key, value):
        async with self._lock:
            self._dict[key] = value

    async def pop(self, key, default=None):
        async with self._lock:
            return self._dict.pop(key, default)

    async def values(self):
        async with self._lock:
            return list(self._dict.values())

    async def keys(self):
        async with self._lock:
            return list(self._dict.keys())

    async def items(self):
        async with self._lock:
            return list(self._dict.items())

    def len_sync(self) -> int:
        return len(self._dict)

    async def __contains__(self, key):
        async with self._lock:
            return key in self._dict

player_sockets = AsyncSafeDict()
player_coords = AsyncSafeDict()

# --- ВАЛИДАЦИЯ ---
class AuthRequest(BaseModel):
    key: str = ""
    hwid: str

    @field_validator('hwid')
    @classmethod
    def validate_hwid(cls, v):
        if not re.match(r"^[a-fA-F0-9]{64}$", v):
            raise ValueError("Invalid HWID format. Must be 64 hex characters (SHA-256).")
        return v.lower()

# --- РАБОТА С БАЗОЙ ДАННЫХ ---
async def init_db():
    global db_pool, db_initialized
    if not DATABASE_URL:
        logging.error("КРИТИЧЕСКАЯ ОШИБКА: DATABASE_URL не найдена.")
        return

    db_pool = await asyncpg.create_pool(DATABASE_URL)

    if not db_initialized:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY,
                    hwid TEXT,
                    banned BOOLEAN DEFAULT FALSE,
                    used BOOLEAN DEFAULT FALSE,
                    expires_at TIMESTAMP,
                    duration_days INT
                )
            """)
            await conn.execute("ALTER TABLE keys ADD COLUMN IF NOT EXISTS used BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP")
            await conn.execute("ALTER TABLE keys ADD COLUMN IF NOT EXISTS duration_days INT")
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    token TEXT PRIMARY KEY,
                    hwid TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            db_initialized = True
            logging.info("База данных инициализирована.")

async def cleanup_sessions():
    while True:
        await asyncio.sleep(3600)
        if db_pool:
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute("DELETE FROM sessions WHERE created_at < NOW() - INTERVAL '1 hour'")
                    logging.info("Очистка старых сессий выполнена.")
            except Exception as e:
                logging.error(f"Ошибка очистки сессий: {e}")

async def self_ping():
    """Фоновая задача для предотвращения засыпания сервера на Render"""
    await asyncio.sleep(15)
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{RENDER_URL}/api/admin/status") as resp:
                    if resp.status == 200:
                        logging.info("Self-ping выполнен успешно.")
                    else:
                        logging.warning(f"Self-ping вернул статус: {resp.status}")
        except Exception as e:
            logging.error(f"Self-ping failed: {e}")
        await asyncio.sleep(600)

# --- LIFESPAN HANDLER ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    await init_db()
    
    cleanup_task = asyncio.create_task(cleanup_sessions())
    polling_task = asyncio.create_task(dp.start_polling(bot))
    ping_task = asyncio.create_task(self_ping())
    
    yield 
    
    cleanup_task.cancel()
    polling_task.cancel()
    ping_task.cancel()
    if db_pool:
        await db_pool.close()
        logging.info("Пул БД закрыт.")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API АВТОРИЗАЦИИ ---
@app.post("/api/auth")
async def authenticate(req: AuthRequest):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")

    async with db_pool.acquire() as conn:
        hwid_row = await conn.fetchrow("SELECT key, banned, expires_at FROM keys WHERE hwid = $1", req.hwid)

        if hwid_row:
            if hwid_row['banned']:
                raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Your HWID is banned"})
            
            if hwid_row['expires_at'] and datetime.now() > hwid_row['expires_at']:
                raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Subscription expired"})

            access_token = uuid.uuid4().hex + uuid.uuid4().hex
            await conn.execute("INSERT INTO sessions (token, hwid) VALUES ($1, $2)", access_token, req.hwid)
            return {"authorized": True, "message": "Welcome back!", "access_token": access_token}

        if not req.key:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "HWID not registered. Key required."})

        key_row = await conn.fetchrow("SELECT hwid, banned, used, duration_days FROM keys WHERE key = $1", req.key)

        if not key_row:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Invalid key"})
        if key_row['banned']:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Key is banned"})
        if key_row['used']:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Key has already been used"})

        duration = key_row['duration_days']
        expires_at = None if duration == -1 else datetime.now() + timedelta(days=duration)

        await conn.execute("UPDATE keys SET hwid = $1, used = TRUE, expires_at = $2 WHERE key = $3", req.hwid, expires_at, req.key)
        
        await bot.send_message(
            ADMIN_ID,
            f"🎉 <b>Активирован новый ключ!</b>\n"
            f"🔑 Ключ: <code>{req.key}</code>\n"
            f"💻 HWID: <code>{req.hwid[:16]}...</code>\n"
            f"⏳ Срок: {'Навсегда' if duration == -1 else f'{duration} дн.'}",
            parse_mode="HTML"
        )

        access_token = uuid.uuid4().hex + uuid.uuid4().hex
        await conn.execute("INSERT INTO sessions (token, hwid) VALUES ($1, $2)", access_token, req.hwid)
        return {"authorized": True, "message": "Key activated and bound to HWID", "access_token": access_token}

# --- WEBSOCKETS ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_username = None
    client_hwid = None

    try:
        auth_data = await asyncio.wait_for(websocket.receive_json(), timeout=10.0)
        
        if auth_data.get("action") != "AUTH":
            await websocket.close(code=1008)
            return
            
        token = auth_data.get("token")
        client_username = auth_data.get("username")
        
        if not token or not client_username:
            await websocket.send_json({"action": "AUTH_FAIL", "reason": "Missing token"})
            await websocket.close(code=1008)
            return

        async with db_pool.acquire() as conn:
            session = await conn.fetchrow(
                "SELECT hwid FROM sessions WHERE token = $1 AND created_at > NOW() - INTERVAL '1 hour'", 
                token
            )
            
            if not session:
                await websocket.send_json({"action": "AUTH_FAIL", "reason": "Invalid or expired token"})
                await websocket.close(code=1008)
                return
                
            client_hwid = session['hwid']

        await player_sockets.set(client_username, websocket)
        logging.info(f"[+] Authenticated WS: {client_username} ({client_hwid})")

        while True:
            data = await asyncio.wait_for(websocket.receive_json(), timeout=30.0)
            action = data.get("action")

            if action == "SEND_COORDINATES":
                coords = data.get("coordinates", {})
                await player_coords.set(client_username, {
                    "serverName": data.get("serverName", "Unknown"),
                    "x": coords.get("x", 0),
                    "y": coords.get("y", 0),
                    "z": coords.get("z", 0),
                    "time": datetime.now().strftime("%H:%M:%S")
                })
            elif action == "PING":
                await websocket.send_json({"action": "PONG"})

    except asyncio.TimeoutError:
        logging.info(f"[!] WS Timeout (no activity): {client_username}")
    except WebSocketDisconnect:
        pass
    finally:
        if client_username:
            await player_sockets.pop(client_username, None)
            await player_coords.pop(client_username, None)
            logging.info(f"[-] Disconnected: {client_username}")

# --- API ДЛЯ ПАНЕЛИ ---
@app.post("/api/admin/request-all")
async def request_all_coords():
    count = 0
    sockets = await player_sockets.values()
    for ws in sockets:
        try:
            await ws.send_json({"action": "GET_COORDINATES"})
            count += 1
        except Exception:
            pass
    return {"status": f"Запрос отправлен. Активно сокетов: {count}"}

@app.get("/api/admin/status")
async def get_status():
    response_data = {}
    keys = await player_sockets.keys()
    for username in keys:
        response_data[username] = {
            "online": True,
            "f3_coordinates": await player_coords.get(username)
        }
    return response_data

# --- TELEGRAM БОТ ---
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def get_main_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🎮 Генерация ключа (1 день)"),
         KeyboardButton(text="🎮 Генерация ключа (30 дней)"),
         KeyboardButton(text="♾️ Генерация Lifetime")],
        [KeyboardButton(text="🔓 Запросить координаты (F3)"),
         KeyboardButton(text="📊 Статистика системы"),
         KeyboardButton(text="👥 Игроки онлайн")],
        [KeyboardButton(text="🚫 Бан ключа"),
         KeyboardButton(text="✅ Разбан ключа"),
         KeyboardButton(text="🔄 Сброс HWID")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder="Выберите действие...")

async def generate_key_logic(duration: int) -> str:
    new_key = str(uuid.uuid4()).split('-')[0]
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO keys (key, hwid, banned, used, duration_days) VALUES ($1, NULL, FALSE, FALSE, $2)", new_key, duration)
    return new_key

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if not is_admin(message.from_user.id): return
    
    ikb = InlineKeyboardBuilder()
    ikb.button(text="📊 Статистика", callback_data="stat")
    ikb.button(text="👥 Игроки онлайн", callback_data="players")
    ikb.button(text="⚡ Запросить координаты", callback_data="get_coords")
    ikb.adjust(2, 1)
    
    text = (
        f"🌐 <b>Система Destra Visuals</b>\n\n"
        f"Данные для Java-клиента мода:\n"
        f"🔗 <b>URL (HTTP):</b> <code>{RENDER_URL}/api/auth</code>\n"
        f"🔗 <b>URL (WS):</b> <code>{WS_URL}/ws</code>\n\n"
        f"ℹ️ <b>Управление доступно кнопками внизу экрана.</b>\n"
        f"Или используй текстовые команды:\n"
        f"<code>/genkey 30</code> | <code>/genkey lifetime</code>\n"
        f"<code>/ban</code> | <code>/unban</code> | <code>/resethwid</code>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=ikb.as_markup())
    await bot.send_message(message.from_user.id, "⌨️ Панель управления активирована.", reply_markup=get_main_keyboard())

@dp.callback_query(F.data == "start")
async def cb_start(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    ikb = InlineKeyboardBuilder()
    ikb.button(text="📊 Статистика", callback_data="stat")
    ikb.button(text="👥 Игроки онлайн", callback_data="players")
    ikb.button(text="⚡ Запросить координаты", callback_data="get_coords")
    ikb.adjust(2, 1)
    await callback.message.edit_text("🌐 <b>Главное меню</b>", parse_mode="HTML", reply_markup=ikb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "stat")
async def cb_stat(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM keys")
        active = await conn.fetchval("SELECT COUNT(*) FROM keys WHERE used = TRUE AND banned = FALSE")
        banned = await conn.fetchval("SELECT COUNT(*) FROM keys WHERE banned = TRUE")
    
    text = (f"📊 <b>Статистика системы</b>\n\n"
            f"Всего ключей: {total}\n"
            f"Активных: {active}\n"
            f"Заблокировано: {banned}\n"
            f"Онлайн в сокетах: {player_sockets.len_sync()}")
    
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="start")
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "players")
async def cb_players(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    if not player_sockets.len_sync():
        kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="start")
        await callback.message.edit_text("Нет активных подключений.", reply_markup=kb.as_markup())
        return

    text = "👥 <b>Активные игроки:</b>\n"
    items = await player_coords.items()
    for user, coords in items:
        if coords:
            text += f"\n👤 <b>{user}</b> [{coords['serverName']}]\n   X:{coords['x']} Y:{coords['y']} Z:{coords['z']} ({coords['time']})"
        else:
            text += f"\n👤 <b>{user}</b> | Координаты не получены"
    
    kb = InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="start")
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "get_coords")
async def cb_get_coords(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id): return
    count = 0
    sockets = await player_sockets.values()
    for ws in sockets:
        try:
            await ws.send_json({"action": "GET_COORDINATES"})
            count += 1
        except Exception:
            pass
    await callback.answer(f"Запрос отправлен {count} игрокам.", show_alert=True)

@dp.message(Command("genkey"))
async def cmd_genkey(message: types.Message):
    if not is_admin(message.from_user.id): return
    if not db_pool: return
    try:
        args = message.text.split()
        days_str = args[1].lower()
        if days_str == "lifetime":
            duration = -1
            days_display = "Навсегда"
        else:
            duration = int(days_str)
            days_display = f"{duration} дн."
    except (IndexError, ValueError):
        return await message.answer("Использование: /genkey 30 или /genkey lifetime", parse_mode="HTML")

    new_key = await generate_key_logic(duration)
    await message.answer(f"✅ Ключ создан: <code>{new_key}</code>\n⏳ Срок: {days_display}", parse_mode="HTML")

@dp.message(Command("resethwid"))
async def cmd_resethwid(message: types.Message):
    if not is_admin(message.from_user.id): return
    if not db_pool: return
    try:
        key = message.text.split()[1]
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE keys SET hwid = NULL, used = FALSE WHERE key = $1", key)
        await message.answer(f"HWID для ключа {key} сброшен.")
    except IndexError:
        await message.answer("Использование: /resethwid <ключ>")

@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if not is_admin(message.from_user.id): return
    if not db_pool: return
    try:
        key = message.text.split()[1]
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE keys SET banned = TRUE WHERE key = $1", key)
        await message.answer(f"Ключ {key} заблокирован.")
    except IndexError:
        await message.answer("Использование: /ban <ключ>")

@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    if not is_admin(message.from_user.id): return
    if not db_pool: return
    try:
        key = message.text.split()[1]
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE keys SET banned = FALSE WHERE key = $1", key)
        await message.answer(f"Ключ {key} разблокирован.")
    except IndexError:
        await message.answer("Использование: /unban <ключ>")

# --- ОБРАБОТКА КНОПОК НИЖНЕЙ КЛАВИАТУРЫ ---
@dp.message(F.text == "🎮 Генерация ключа (1 день)")
async def kb_genkey_1(message: types.Message):
    if not is_admin(message.from_user.id): return
    new_key = await generate_key_logic(1)
    await message.answer(f"✅ Ключ создан: <code>{new_key}</code>\n⏳ Срок: 1 дн.", parse_mode="HTML")

@dp.message(F.text == "🎮 Генерация ключа (30 дней)")
async def kb_genkey_30(message: types.Message):
    if not is_admin(message.from_user.id): return
    new_key = await generate_key_logic(30)
    await message.answer(f"✅ Ключ создан: <code>{new_key}</code>\n⏳ Срок: 30 дн.", parse_mode="HTML")

@dp.message(F.text == "♾️ Генерация Lifetime")
async def kb_genkey_life(message: types.Message):
    if not is_admin(message.from_user.id): return
    new_key = await generate_key_logic(-1)
    await message.answer(f"✅ Ключ создан: <code>{new_key}</code>\n⏳ Срок: Навсегда", parse_mode="HTML")

@dp.message(F.text == "🔓 Запросить координаты (F3)")
async def kb_get_coords(message: types.Message):
    if not is_admin(message.from_user.id): return
    count = 0
    sockets = await player_sockets.values()
    for ws in sockets:
        try:
            await ws.send_json({"action": "GET_COORDINATES"})
            count += 1
        except Exception:
            pass
    await message.answer(f"⚡ Команда GET_COORDINATES отправлена {count} игрокам.")

@dp.message(F.text == "📊 Статистика системы")
async def kb_stat(message: types.Message):
    if not is_admin(message.from_user.id): return
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM keys")
        active = await conn.fetchval("SELECT COUNT(*) FROM keys WHERE used = TRUE AND banned = FALSE")
        banned = await conn.fetchval("SELECT COUNT(*) FROM keys WHERE banned = TRUE")
    
    text = (f"📊 <b>Статистика системы</b>\n\n"
            f"Всего ключей: {total}\n"
            f"Активных: {active}\n"
            f"Заблокировано: {banned}\n"
            f"Онлайн в сокетах: {player_sockets.len_sync()}")
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "👥 Игроки онлайн")
async def kb_players(message: types.Message):
    if not is_admin(message.from_user.id): return
    if not player_sockets.len_sync():
        return await message.answer("Нет активных подключений.")

    text = "👥 <b>Активные игроки:</b>\n"
    items = await player_coords.items()
    for user, coords in items:
        if coords:
            text += f"\n👤 <b>{user}</b> [{coords['serverName']}]\n   X:{coords['x']} Y:{coords['y']} Z:{coords['z']} ({coords['time']})"
        else:
            text += f"\n👤 <b>{user}</b> | Координаты не получены"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(F.text.in_(["🚫 Бан ключа", "✅ Разбан ключа", "🔄 Сброс HWID"]))
async def kb_request_key(message: types.Message):
    if not is_admin(message.from_user.id): return
    action = message.text
    await message.answer(
        f"Вы выбрали: <b>{action}</b>.\n"
        f"Пожалуйста, отправьте ключ в формате:\n"
        f"<code>{action.split()[0]} YOUR_KEY</code>",
        parse_mode="HTML"
    )

# Раздача статических файлов
os.makedirs("public", exist_ok=True)
app.mount("/panel", StaticFiles(directory="public", html=True), name="public")

# --- ЗАПУСК ВСЕГО ВМЕСТЕ ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info", reload=False)
