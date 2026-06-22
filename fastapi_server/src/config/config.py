from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE = PROJECT_ROOT / "env" / ".env.api"

# File này có thể không tồn tại trong Docker.
# Khi chạy Docker, Compose đã đưa biến vào container.
load_dotenv(
    dotenv_path=ENV_FILE,
    override=False,
)


def require_env(name: str) -> str:
    """
    Đọc một biến môi trường bắt buộc.

    Báo lỗi ngay khi cấu hình bị thiếu thay vì để ứng dụng
    lỗi ở bước kết nối database hoặc Redis.
    """
    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(
            f"Thiếu biến môi trường bắt buộc: {name}"
        )

    return value.strip()


@dataclass(frozen=True)
class Settings:
    database_url: str | None
    sql_server_driver: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    trust_db_server: str
    db_pool: str
    db_max_overflow: str

    redis_url: str
    redis_max_connect: str
    redis_timeout: str
    websocket_ticket_ttl: str

    upload_directory: str
    app_update_dir: str
    api_log_directory: str
    system_log_directory: str


@lru_cache
def get_settings() -> Settings:
    return Settings(
        database_url=os.getenv("DATABASE_URL"),
        sql_server_driver=require_env("SQL_SERVER_DRIVER"),
        db_host=require_env("SQL_SERVER_HOST"),
        db_port=int(os.getenv("SQL_SERVER_PORT", "1433")),
        db_user=require_env("SQL_SERVER_USERNAME"),
        db_password=require_env("SQL_SERVER_PASSWORD"),
        db_name=require_env("SQL_SERVER_DATABASE"),
        trust_db_server=require_env("SQL_SERVER_TRUST_CERTIFICATE"),
        db_pool=os.getenv("DB_POOL", "10"),
        db_max_overflow=os.getenv("DB_MAX_OVERFLOW", "20"),
        redis_url=require_env("REDIS_URL"),
        redis_max_connect= os.getenv("REDIS_MAX_CONNECTIONS", "200"),
        redis_timeout= os.getenv("REDIS_STREAM_SOCKET_TIMEOUT", "200"),
        websocket_ticket_ttl= os.getenv("WS_TICKET_TTL_SECONDS", "60"),
        upload_directory=os.getenv( "UPLOAD_DIRECTORY", "/app/uploads"),
        app_update_dir=os.getenv( "APP_UPDATE_DIR", "/app/update_application"),
        api_log_directory=os.getenv( "API_LOG_DIRECTORY", "/app/log/api_log"),
        system_log_directory=os.getenv( "SYSTEM_LOG_DIRECTORY", "/app/log/system_log"),
    )


settings = get_settings()