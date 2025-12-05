# MD Ticket Project

Airflow: http://87.242.101.90:8080

## 1. Запуск проекта в Docker

1. Скопируйте переменные окружения и при необходимости поправьте их:
   ```bash
   make init-env   # или cp .env.example .env
   ```
   Затем замените значения переменных
2. Поднимите весь стек (Postgres, MongoDB, FastAPI, Airflow и вспомогательные сервисы):
   ```bash
   make up   # или docker compose up -d
   ```
3. Проверьте логи конкретного сервиса, если нужно:
   ```bash
   docker compose logs -f md-ticket-stack-backend_app-1
   ```
4. Остановить и удалить контейнеры можно командами `make down` / `docker compose down` или `make clean` / `docker compose down -v --remove-orphans`.

## 2. Установка и запуск pre-commit

1. Создайте и активируйте виртуальное окружение (опционально):
   ```bash
   python -m venv .venv && source .venv/bin/activate
   ```
2. Установите dev-зависимости (включая сам `pre-commit`):
   ```bash
   pip install -r requirements-dev.txt   # либо pip install pre-commit
   ```
3. Повесьте git-хук локально:
   ```bash
   pre-commit install
   ```
4. Чтобы вручную проверить всё дерево, запустите:
   ```bash
   pre-commit run --all-files
   ```

## 3. Что выполняет pre-commit

Файл `.pre-commit-config.yaml` хранит набор хуков, которые выполняются перед каждым `git commit`:

- **black** — форматирование Python-кода по единым правилам.
- **ruff --fix** — быстрый линтер и автофикс типовых ошибок.
- **pyupgrade --py310-plus** — обновляет синтаксис под минимальную версию Python 3.10.
- **mypy** — статический анализ аннотаций типов.
- **bandit -lll** — поиск критичных уязвимостей и небезопасных конструкций.

## 4. Сервисы и порты/URL

| Сервис                | URL / Порт                        | Назначение |
|-----------------------|-----------------------------------|------------|
| FastAPI Backend       | `http://localhost:${FASTAPI_PORT:-8000}` | Основное API приложения (`/api/*`). |
| Health-check          | `http://localhost:${FASTAPI_PORT:-8000}/api/health` | Проверка доступности Postgres и MongoDB. |
| Postgres              | `localhost:${POSTGRES_PORT:-5432}` | База данных приложения (пользователь/БД из `.env`). |
| MongoDB               | `localhost:${MONGO_PORT:-27017}`  | Хранилище событий/логов (логин/пароль из `.env`). |
| Airflow Webserver     | `http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}` | UI оркестратора (логин/пароль `AIRFLOW_WWW_USER_*`). |
| Airflow Redis (broker)| `localhost:${AIRFLOW_REDIS_PORT:-6380}` | Брокер задач Celery внутри Airflow (для отладки). |

> Остальные сервисы (Airflow Scheduler/Worker/Triggerer/Postgres) работают внутри сети `md-ticket-core` и намеренно не проброшены наружу.

## 5. Переменные окружения

| Переменная | Значение по умолчанию | Описание |
|------------|-----------------------|----------|
| `FASTAPI_PORT` | `8000` | Порт, на котором backend доступен с хоста. |
| `POSTGRES_USER` | `md_ticket_user` | Логин для Postgres приложения. |
| `POSTGRES_PASSWORD` | `md_ticket_password` | Пароль пользователя Postgres. |
| `POSTGRES_DB` | `md_ticket` | Имя базы данных приложения. |
| `POSTGRES_PORT` | `5432` | Проброшенный порт Postgres на хосте. |
| `MONGO_INITDB_ROOT_USERNAME` | `md_ticket_mongo` | Пользователь администратора MongoDB. |
| `MONGO_INITDB_ROOT_PASSWORD` | `md_ticket_mongo_password` | Пароль администратора MongoDB. |
| `MONGO_INITDB_DATABASE` | `md_ticket` | База данных MongoDB, создаваемая при инициализации. |
| `MONGO_PORT` | `27017` | Порт MongoDB, доступный с хоста. |
| `AIRFLOW_UID` | `50000` | UID пользователя, от которого запускаются процессы Airflow в контейнере. |
| `AIRFLOW_FERNET_KEY` | _(пусто)_ | Ключ для шифрования соединений в Airflow (обязательно заполнить в проде). |
| `AIRFLOW_PARALLELISM` | `16` | Лимит параллельных тасков Airflow. |
| `AIRFLOW_WEBSERVER_PORT` | `8080` | Порт UI Airflow на хосте. |
| `AIRFLOW_REDIS_PORT` | `6380` | Проброшенный порт Redis, который служит брокером для Airflow. |
| `AIRFLOW_DB_USER` | `airflow` | Пользователь Postgres для Airflow. |
| `AIRFLOW_DB_PASSWORD` | `airflow_password` | Пароль Airflow Postgres. |
| `AIRFLOW_DB_NAME` | `airflow` | Имя базы данных Airflow. |
| `AIRFLOW_WWW_USER_USERNAME` | `admin` | Логин администратора для входа в Airflow UI. |
| `AIRFLOW_WWW_USER_PASSWORD` | `admin` | Пароль администратора Airflow UI. |

> После изменения `.env` перезапустите контейнеры, чтобы сервисы прочитали новые значения.

