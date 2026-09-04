import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, date, timezone
from typing import List, Optional, Literal

import httpx
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field, ConfigDict
from pydantic_settings import BaseSettings

# 1. Конфигурация
class Settings(BaseSettings):
    app_host: str = "127.0.0.1"
    app_port: int = 8000
    open_meteo_url: str = "https://api.open-meteo.com/v1/forecast"
    db_file: str = "weather.db"
    update_interval_seconds: int = 900
    request_timeout: float = 10.0

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )

settings = Settings()

# 2. Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("weather_api")

# 3. Работа с базой данных (SQLite)
def get_db():
    conn = sqlite3.connect(settings.db_file, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with sqlite3.connect(settings.db_file) as conn:
        cursor = conn.cursor()

        # 1) Таблица пользователей
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2) Таблица городов (привязана к user_id)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, name),
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """)

        # 3) Таблица почасовых прогнозов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS forecasts (
                city_id INTEGER NOT NULL,
                forecast_date TEXT NOT NULL,
                forecast_time TEXT NOT NULL,
                temperature REAL,
                humidity REAL,
                wind_speed REAL,
                precipitation REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (city_id, forecast_date, forecast_time),
                FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE
            )
        """)
        conn.commit()
    logger.info("База данных инициализирована (Multi-user schema).")


# 4. Pydantic-модели
class CurrentWeatherResponse(BaseModel):
    latitude: float
    longitude: float
    temperature_c: float
    wind_speed_kmh: float
    pressure_hpa: float
    observation_time: str

class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="Имя пользователя")

class UserResponse(BaseModel):
    user_id: int
    username: str

class CityTrackRequest(BaseModel):
    city_name: str = Field(..., min_length=1, max_length=100, description="Название города")
    latitude: float = Field(..., ge=-90, le=90, description="Широта")
    longitude: float = Field(..., ge=-180, le=180, description="Долгота")

class CityResponse(BaseModel):
    city_name: str
    latitude: float
    longitude: float


# 5. Создание FastAPI-приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(update_forecasts_background_task())
    yield
    task.cancel()

app = FastAPI(
    title="Weather API Service (Multi-user)",
    description="REST API для получения погоды с поддержкой нескольких пользователей",
    lifespan=lifespan
)

# 6. Фоновая задача (обновление каждые 15 минут)
async def update_forecasts_background_task():
    logger.info("Фоновая задача обновления запущена.")
    while True:
        await asyncio.sleep(settings.update_interval_seconds)
        logger.info(f"[ФОН] Начало планового обновления прогнозов...")

        try:
            with sqlite3.connect(settings.db_file) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT id, name, latitude, longitude FROM cities")
                cities = cursor.fetchall()

            async with httpx.AsyncClient(timeout=settings.request_timeout) as client:
                for city in cities:
                    try:
                        await fetch_and_save_forecast(client, city["id"], city["latitude"], city["longitude"])
                    except Exception as e:
                        logger.error(f"[ФОН] Ошибка обновления для {city['name']}: {e}")
        except Exception as e:
            logger.error(f"[ФОН] Критическая ошибка в цикле обновления: {e}")

async def fetch_and_save_forecast(client: httpx.AsyncClient, city_id: int, lat: float, lon: float):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "timezone": "auto",
        "forecast_days": 1
    }
    response = await client.get(settings.open_meteo_url, params=params)
    response.raise_for_status()
    data = response.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    hums = hourly.get("relative_humidity_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    precs = hourly.get("precipitation", [])

    forecast_date = date.today().isoformat()

    with sqlite3.connect(settings.db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM forecasts WHERE city_id = ? AND forecast_date = ?", (city_id, forecast_date))

        insert_data = [
            (city_id, forecast_date, t.split("T")[1], temp, hum, wind, prec)
            for t, temp, hum, wind, prec in zip(times, temps, hums, winds, precs)
        ]
        cursor.executemany("""
            INSERT INTO forecasts (city_id, forecast_date, forecast_time, temperature, humidity, wind_speed, precipitation)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, insert_data)
        conn.commit()


# 7. Эндпоинты API

@app.post("/users/register", response_model=UserResponse, status_code=201, summary="Регистрация пользователя")
async def register_user(user_req: UserCreateRequest, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute("INSERT INTO users (username) VALUES (?)", (user_req.username,))
        db.commit()
        logger.info(f"Зарегистрирован новый пользователь: {user_req.username} (ID: {cursor.lastrowid})")
        return UserResponse(user_id=cursor.lastrowid, username=user_req.username)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Имя пользователя уже занято")


@app.get("/weather/current", response_model=CurrentWeatherResponse, summary="Текущая погода по координатам")
async def get_current_weather(
        latitude: float = Query(..., ge=-90, le=90),
        longitude: float = Query(..., ge=-180, le=180),
):
    logger.info(f"Запрос текущей погоды: lat={latitude}, lon={longitude}")
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,wind_speed_10m,surface_pressure",
        "timezone": "auto",
    }
    try:
        async with httpx.AsyncClient(timeout=settings.request_timeout) as client:
            response = await client.get(settings.open_meteo_url, params=params)
            response.raise_for_status()
            data = response.json()
    except httpx.RequestError as e:
        logger.error(f"Ошибка сети: {e}")
        raise HTTPException(status_code=502, detail="Не удалось связаться с внешним сервисом")

    current = data.get("current", {})
    return CurrentWeatherResponse(
        latitude=data.get("latitude", latitude),
        longitude=data.get("longitude", longitude),
        temperature_c=current.get("temperature_2m"),
        wind_speed_kmh=current.get("wind_speed_10m"),
        pressure_hpa=current.get("surface_pressure"),
        observation_time=current.get("time"),
    )


@app.post("/cities/track", status_code=201, summary="Добавить город в отслеживание")
async def track_city(
        user_id: int = Query(..., description="ID пользователя"),
        city_req: CityTrackRequest = ...,
        db: sqlite3.Connection = Depends(get_db)
):
    cursor = db.cursor()

    cursor.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="Пользователь не найден. Сначала зарегистрируйтесь.")

    city_name_normalized = city_req.city_name.strip().lower()

    try:
        cursor.execute("""
            INSERT INTO cities (user_id, name, latitude, longitude) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, name) DO UPDATE SET 
            latitude=excluded.latitude, longitude=excluded.longitude
        """, (user_id, city_name_normalized, city_req.latitude, city_req.longitude))

        city_id = cursor.lastrowid
        if not city_id:
            cursor.execute("SELECT id FROM cities WHERE user_id = ? AND name = ?", (user_id, city_name_normalized))
            city_id = cursor.fetchone()["id"]

        db.commit()
    except Exception as e:
        logger.error(f"Ошибка БД при добавлении города: {e}")
        raise HTTPException(status_code=500, detail="Ошибка базы данных")

    try:
        async with httpx.AsyncClient(timeout=settings.request_timeout) as client:
            await fetch_and_save_forecast(client, city_id, city_req.latitude, city_req.longitude)
        logger.info(f"Пользователь {user_id} добавил город '{city_req.city_name}'")
    except Exception as e:
        logger.warning(f"Не удалось сразу загрузить прогноз для {city_req.city_name}: {e}")

    return {"message": f"Город '{city_req.city_name}' успешно добавлен для пользователя {user_id}"}


@app.get("/cities", response_model=List[CityResponse], summary="Список городов пользователя")
async def get_tracked_cities(
    user_id: int = Query(..., description="ID пользователя"),
    db: sqlite3.Connection = Depends(get_db)
):
    cursor = db.cursor()
    cursor.execute("SELECT name, latitude, longitude FROM cities WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()

    return [
        CityResponse(
            city_name=row["name"],
            latitude=row["latitude"],
            longitude=row["longitude"]
        )
        for row in rows
    ]


@app.get("/weather/city/{city_name}", summary="Прогноз в городе на конкретное время")
async def get_city_weather_at_time(
        city_name: str,
        user_id: int = Query(..., description="ID пользователя"),
        time: str = Query(..., description="Время в формате ЧЧ:ММ (например, 14:00)"),
        temperature: bool = Query(False, description="Включить температуру"),
        humidity: bool = Query(False, description="Включить влажность"),
        wind_speed: bool = Query(False, description="Включить скорость ветра"),
        precipitation: bool = Query(False, description="Включить осадки"),
        db: sqlite3.Connection = Depends(get_db)
):

    if len(time) != 5 or time[2] != ':':
        raise HTTPException(status_code=400, detail="Неверный формат времени. Используйте ЧЧ:ММ")

    if not any([temperature, humidity, wind_speed, precipitation]):
        raise HTTPException(status_code=400, detail="Выберите хотя бы один параметр погоды")

    city_name_normalized = city_name.strip().lower()
    cursor = db.cursor()

    cursor.execute("SELECT id, name FROM cities WHERE name = ? AND user_id = ?", (city_name_normalized, user_id))
    city = cursor.fetchone()
    if not city:
        raise HTTPException(
            status_code=404,
            detail=f"Город '{city_name}' не найден у пользователя {user_id}. Добавьте его через POST /cities/track"
        )

    today_str = date.today().isoformat()

    cursor.execute("""
        SELECT temperature, humidity, wind_speed, precipitation 
        FROM forecasts 
        WHERE city_id = ? AND forecast_date = ? AND forecast_time = ?
    """, (city["id"], today_str, time))
    forecast = cursor.fetchone()

    if not forecast:
        raise HTTPException(
            status_code=404,
            detail=f"Прогноз на {today_str} {time} для города '{city['name']}' не найден."
        )

    result = {
        "city_name": city["name"],
        "date": today_str,
        "time": time
    }

    if temperature:
        result["temperature_c"] = forecast["temperature"]
    if humidity:
        result["humidity_percent"] = forecast["humidity"]
    if wind_speed:
        result["wind_speed_kmh"] = forecast["wind_speed"]
    if precipitation:
        result["precipitation_mm"] = forecast["precipitation"]

    return result


# 8. Точка входа
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Запуск сервера на {settings.app_host}:{settings.app_port}/docs")
    uvicorn.run(app, host=settings.app_host, port=settings.app_port)
