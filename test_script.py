"""
Юнит-тесты для Weather API Service.
Проверяют логику API в изоляции: с БД в оперативной памяти и заглушками (mocks) для внешних API.
Запуск: pytest test_script.py -v
"""
import pytest
import sqlite3
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from script import app, get_db

# 1. Фикстура для изолированной БД в памяти на каждый тест

@pytest.fixture(autouse=True)
def setup_test_db():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    cursor.execute("CREATE TABLE cities (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, name TEXT NOT NULL, latitude REAL NOT NULL, longitude REAL NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id, name))")
    cursor.execute("CREATE TABLE forecasts (city_id INTEGER NOT NULL, forecast_date TEXT NOT NULL, forecast_time TEXT NOT NULL, temperature REAL, humidity REAL, wind_speed REAL, precipitation REAL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (city_id, forecast_date, forecast_time))")
    conn.commit()

    def override_get_db():
        yield conn

    app.dependency_overrides[get_db] = override_get_db

    yield conn  

    app.dependency_overrides = {}
    conn.close()


client = TestClient(app)


# 2. Фиктивные данные (Mocks) для Open-Meteo

MOCK_CURRENT_RESPONSE = {
    "latitude": 55.75,
    "longitude": 37.61,
    "current": {
        "time": "2023-10-25T12:00",
        "temperature_2m": 15.5,
        "wind_speed_10m": 10.2,
        "surface_pressure": 1015.0
    }
}

MOCK_HOURLY_RESPONSE = {
    "hourly": {
        "time": ["2023-10-25T12:00", "2023-10-25T13:00"],
        "temperature_2m": [15.5, 16.0],
        "relative_humidity_2m": [60, 58],
        "wind_speed_10m": [10.2, 11.0],
        "precipitation": [0.0, 0.0]
    }
}

def mock_open_meteo_current(*args, **kwargs):
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_CURRENT_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    return mock_resp

def mock_open_meteo_hourly(*args, **kwargs):
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_HOURLY_RESPONSE
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


# 3. Юнит-тесты

@patch("httpx.AsyncClient.get", side_effect=mock_open_meteo_current)
def test_get_current_weather(mock_get):
    """Тест 1: Метод возвращает распарсенные данные текущей погоды."""
    response = client.get("/weather/current?latitude=55.75&longitude=37.61")

    assert response.status_code == 200
    data = response.json()
    assert data["temperature_c"] == 15.5
    assert data["wind_speed_kmh"] == 10.2
    assert data["pressure_hpa"] == 1015.0
    assert mock_get.call_count == 1


def test_register_user():
    """Тест 2: Успешная регистрация пользователя."""
    response = client.post("/users/register", json={"username": "test_user"})

    assert response.status_code == 201
    data = response.json()
    assert data["user_id"] == 1
    assert data["username"] == "test_user"


def test_register_user_duplicate():
    """Тест 3: Ошибка при регистрации существующего пользователя."""
    client.post("/users/register", json={"username": "duplicate_user"})
    response = client.post("/users/register", json={"username": "duplicate_user"})

    assert response.status_code == 409
    assert "уже занято" in response.json()["detail"]


@patch("httpx.AsyncClient.get", side_effect=mock_open_meteo_hourly)
def test_track_city(mock_get, setup_test_db):
    """Тест 4: Добавление города и проверка, что он попал в БД."""
    reg_resp = client.post("/users/register", json={"username": "city_tester"})
    user_id = reg_resp.json()["user_id"]

    response = client.post(
        "/cities/track",
        params={"user_id": user_id},
        json={"city_name": "London", "latitude": 51.5, "longitude": -0.1}
    )

    assert response.status_code == 201
    assert "успешно добавлен" in response.json()["message"]

    cursor = setup_test_db.cursor()
    cursor.execute("SELECT name FROM cities WHERE user_id = ?", (user_id,))
    assert cursor.fetchone()["name"] == "london"


def test_get_city_weather_at_time_filtered(setup_test_db):
    """Тест 5: Возврат только запрошенных параметров погоды."""
    reg_resp = client.post("/users/register", json={"username": "weather_checker"})
    user_id = reg_resp.json()["user_id"]

    client.post(
        "/cities/track",
        params={"user_id": user_id},
        json={"city_name": "Paris", "latitude": 48.8, "longitude": 2.3}
    )

    cursor = setup_test_db.cursor()
    cursor.execute("SELECT id FROM cities WHERE name = 'paris' AND user_id = ?", (user_id,))
    city_id = cursor.fetchone()["id"]

    from datetime import date
    today = date.today().isoformat()

    cursor.execute("""
        INSERT INTO forecasts (city_id, forecast_date, forecast_time, temperature, humidity, wind_speed, precipitation)
        VALUES (?, ?, '12:00', 20.5, 65.0, 15.0, 0.0)
    """, (city_id, today))
    setup_test_db.commit()

    response = client.get(
        "/weather/city/paris",
        params={
            "user_id": user_id,
            "time": "12:00",
            "temperature": True,
            "wind_speed": True,
            "humidity": False,
            "precipitation": False
        }
    )

    assert response.status_code == 200
    data = response.json()

    assert "temperature_c" in data
    assert data["temperature_c"] == 20.5
    assert "wind_speed_kmh" in data
    assert data["wind_speed_kmh"] == 15.0


    assert "humidity_percent" not in data
    assert "precipitation_mm" not in data

def test_get_weather_invalid_time_format():
    """Тест 6: Валидация формата времени (должна быть ошибка 400)."""
    response = client.get(
        "/weather/city/london",
        params={"user_id": 999, "time": "1200", "params": ["temperature"]}
    )

    assert response.status_code == 400
    assert "Неверный формат времени" in response.json()["detail"]
