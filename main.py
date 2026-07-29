import asyncio
import logging
import uuid
import os
import asyncpg
from datetime import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import uvicorn

# --- НАСТРОЙКИ ---
BOT_TOKEN = "7723298001:AAEqIhvfOo-uoi5keS6--a5mfFc9gC4oL-I"
ADMIN_ID = 1186053117  # Твой Telegram ID

# Получаем URL базы данных и внешний домен от Render
DATABASE_URL = os.environ.get("DATABASE_URL")
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:8000")
WS_URL = RENDER_URL.replace("http://", "ws://").replace("https://", "wss://")

# Инициализация
app = FastAPI()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

# Глобальный пул соединений с базой данных
db_pool = None

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ХРАНИЛИЩЕ DESTRA VISUALS (WebSockets) ---
player_sockets: dict[str, WebSocket] = {}
player_coords: dict[str, dict] = {}


class AuthRequest(BaseModel):
    key: str = ""
    hwid: str


# --- РАБОТА С БАЗОЙ ДАННЫХ ---
async def init_db():
    global db_pool
    if not DATABASE_URL:
        logging.error("КРИТИЧЕСКАЯ ОШИБКА: DATABASE_URL не найдена. Укажи её в Environment Variables на Render!")
        return

    # Создаем пул подключений к Postgres
    db_pool = await asyncpg.create_pool(DATABASE_URL)

    async with db_pool.acquire() as conn:
        # В Postgres используются нормальные BOOLEAN значения
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS keys (
                key TEXT PRIMARY KEY,
                hwid TEXT,
                banned BOOLEAN DEFAULT FALSE,
                used BOOLEAN DEFAULT FALSE
            )
        """)
        # Безопасное добавление колонки (если вдруг её нет в старой базе)
        await conn.execute("ALTER TABLE keys ADD COLUMN IF NOT EXISTS used BOOLEAN DEFAULT FALSE")


# --- API АВТОРИЗАЦИИ (HWID) ---
@app.post("/api/auth")
async def authenticate(req: AuthRequest):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database not connected")

    async with db_pool.acquire() as conn:
        # В Postgres переменные передаются через $1, $2, а не через ?
        hwid_row = await conn.fetchrow("SELECT banned FROM keys WHERE hwid = $1", req.hwid)

        if hwid_row:
            if hwid_row['banned']:
                raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Your HWID is banned"})
            return {"authorized": True, "message": "HWID recognized. Welcome back!"}

        if not req.key:
            raise HTTPException(status_code=403,
                                detail={"authorized": False, "reason": "HWID not registered. Key required."})

        key_row = await conn.fetchrow("SELECT hwid, banned, used FROM keys WHERE key = $1", req.key)

        if not key_row:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Invalid key"})

        saved_hwid, banned, used = key_row['hwid'], key_row['banned'], key_row['used']

        if banned:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Key is banned"})
        if used:
            raise HTTPException(status_code=403, detail={"authorized": False, "reason": "Key has already been used"})

        await conn.execute("UPDATE keys SET hwid = $1, used = TRUE WHERE key = $2", req.hwid, req.key)
        return {"authorized": True, "message": "Key activated and bound to HWID"}


# --- WEBSOCKETS (DESTRA VISUALS) ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_username = None
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "AUTH":
                client_username = data.get("username")
                if client_username:
                    player_sockets[client_username] = websocket
                    logging.info(f"[+] Подключился мод игрока: {client_username}")

            elif action == "SEND_COORDINATES":
                if client_username:
                    coords = data.get("coordinates", {})
                    player_coords[client_username] = {
                        "serverName": data.get("serverName", "Неизвестно"),
                        "x": coords.get("x", 0),
                        "y": coords.get("y", 0),
                        "z": coords.get("z", 0),
                        "time": datetime.now().strftime("%H:%M:%S")
                    }
    except WebSocketDisconnect:
        if client_username:
            player_sockets.pop(client_username, None)
            player_coords.pop(client_username, None)
            logging.info(f"[-] Отключился игрок: {client_username}")


# --- API DESTRA VISUALS ДЛЯ ПАНЕЛИ ---
@app.post("/api/admin/request-all")
async def request_all_coords():
    count = 0
    for ws in player_sockets.values():
        await ws.send_json({"action": "GET_COORDINATES"})
        count += 1
    return {"status": f"Запрос отправлен. Активно сокетов: {count}"}


@app.get("/api/admin/status")
async def get_status():
    response_data = {}
    for username in player_sockets.keys():
        response_data[username] = {
            "online": True,
            "f3_coordinates": player_coords.get(username)
        }
    return response_data


# --- TELEGRAM БОТ ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    text = (
        f"🌐 **Система Destra Visuals (Render + Postgres) запущена!**\n\n"
        f"Данные для Java-клиента мода:\n"
        f"🔗 **URL (HTTP):** `{RENDER_URL}/api/auth`\n"
        f"🔗 **URL (WS):** `{WS_URL}/ws`\n\n"
        "**Команды авторизации:**\n/genkey — создать ключ\n/ban <key> — забанить\n/unban <key> — разбанить\n/resethwid <key> — сброс\n\n"
        "**Команды радара:**\n/players — список\n/getcoords — запросить F3"
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message(Command("players"))
async def cmd_players(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if not player_sockets:
        return await message.answer("Нет активных подключений.")

    text = "Активные игроки:\n"
    for user in player_sockets.keys():
        coords = player_coords.get(user)
        if coords:
            text += f"👤 {user} [{coords['serverName']}] | X:{coords['x']} Y:{coords['y']} Z:{coords['z']} (Обн. {coords['time']})\n"
        else:
            text += f"👤 {user} | Координаты не получены\n"
    await message.answer(text)


@dp.message(Command("getcoords"))
async def cmd_getcoords(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    count = 0
    for ws in player_sockets.values():
        await ws.send_json({"action": "GET_COORDINATES"})
        count += 1
    await message.answer(f"Команда GET_COORDINATES отправлена {count} игрокам.")


@dp.message(Command("genkey"))
async def cmd_genkey(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    if not db_pool: return await message.answer("Ошибка: нет подключения к БД")

    new_key = str(uuid.uuid4()).split('-')[0]
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO keys (key, hwid, banned, used) VALUES ($1, NULL, FALSE, FALSE)", new_key)
    await message.answer(f"Новый одноразовый ключ создан: `{new_key}`\nОтправь его игроку.", parse_mode="Markdown")


@dp.message(Command("resethwid"))
async def cmd_resethwid(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
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
    if message.from_user.id != ADMIN_ID: return
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
    if message.from_user.id != ADMIN_ID: return
    if not db_pool: return
    try:
        key = message.text.split()[1]
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE keys SET banned = FALSE WHERE key = $1", key)
        await message.answer(f"Ключ {key} разблокирован.")
    except IndexError:
        await message.answer("Использование: /unban <ключ>")


# Раздача статических файлов (веб-панель)
os.makedirs("public", exist_ok=True)
app.mount("/panel", StaticFiles(directory="public", html=True), name="public")


# --- ЗАПУСК ВСЕГО ВМЕСТЕ ---
async def main():
    await init_db()
    # Render передает нужный порт через переменную окружения PORT
    port = int(os.environ.get("PORT", 8000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)

    await asyncio.gather(
        server.serve(),
        dp.start_polling(bot)
    )


if __name__ == "__main__":
    asyncio.run(main())