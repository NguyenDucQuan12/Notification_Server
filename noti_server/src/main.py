"""
Điểm khởi động của Notification Server.

Notification Server làm 1 việc: đọc event từ Redis Stream (do API server và worker ghi vào) và đẩy realtime tới client qua WebSocket.
Nó KHÔNG truy cập DB, KHÔNG tạo ticket (việc của API server).

Cách chạy (đứng tại thư mục Notification_Server):
    python noti_server\src\main.py
Hoặc dùng uvicorn trực tiếp:
    uvicorn main:app --host 0.0.0.0 --port 1234
"""

from __future__ import annotations
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.configs import settings
from core.logging_config import UVICORN_LOG_CONFIG, setup_logging

# Phải cấu hình logging TRƯỚC khi import các module khác để log lúc import cũng đúng định dạng.
setup_logging(level=settings.LOG_LEVEL, json_logs=settings.log_json)

from api import health_api, noti_api  # noqa: E402
from services.connection_manager import connection_manager  # noqa: E402
from services.redis_client import close_redis_clients, ping_redis  # noqa: E402
from services.stream_hub import stream_hub  # noqa: E402

logger = logging.getLogger("noti.main")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """
    Vòng đời ứng dụng.

    Trước yield: chạy 1 lần khi khởi động.
    Sau yield  : chạy 1 lần khi tắt (Ctrl+C, docker stop, deploy bản mới).
    """
    logger.info(
        "Notification service starting",
        extra={"version": settings.APP_VERSION, "environment": settings.ENVIRONMENT},
    )

    # Kiểm tra Redis ngay khi khởi động để phát hiện cấu hình sai sớm.
    # Chỉ CẢNH BÁO, không dừng server: Redis có thể khởi động chậm hơn (docker-compose),
    # trong lúc đó /health/ready trả 503 nên load balancer sẽ chưa chuyển client vào.
    if await ping_redis():
        logger.info("Redis đã sẵn sàng tại startup; readiness sẽ báo ready")
    else:
        logger.warning("Không kết nối được Redis tại startup; readiness sẽ báo not ready")

    # Khởi động các shard đọc Redis Stream dùng chung (Stream Hub).
    # Shard tự thử lại nếu Redis chưa sẵn sàng, nên không cần chờ Redis ở đây.
    stream_hub.start()

    yield

    # --- Tắt server an toàn ---
    logger.info("Notification service sẽ sớm tắt, dừng các shard và đóng WebSocket còn lại")
    # 1. Đóng các WebSocket còn lại với code 1012 để client reconnect sang instance khác.
    await connection_manager.shutdown()
    # 2. Dừng các shard của Hub.
    await stream_hub.stop()
    # 3. Đóng kết nối Redis.
    await close_redis_clients()
    logger.info("Notification service đã tắt xong")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
    root_path=settings.ROOT_PATH,
    # Tắt Swagger ở production để không lộ cấu trúc API ra ngoài.
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url=None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
)


# CORS chỉ có tác dụng với request HTTP (vd /health), KHÔNG áp dụng cho WebSocket.
# Bảo vệ WebSocket bằng ticket + kiểm tra Origin (WS_ALLOWED_ORIGINS).
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET"],      # Server này chỉ có endpoint GET (health) + WebSocket.
    allow_headers=["*"],
)

# Các router của API server.
app.include_router(health_api.router)
app.include_router(noti_api.router)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    """
    Endpoint gốc, trả về thông tin service.
    """
    return {"service": settings.APP_NAME, "status": "running"}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.HOST,
        port=settings.PORT,
        log_config=UVICORN_LOG_CONFIG,                    # Dùng cấu hình log của mình.
        ws_max_size=settings.WS_MAX_MESSAGE_SIZE,         # Chặn client gửi message khổng lồ (mặc định 16MB).
        ws_ping_interval=settings.WS_PING_INTERVAL,       # Ping tầng giao thức, phát hiện kết nối chết.
        ws_ping_timeout=settings.WS_PING_TIMEOUT,
        timeout_graceful_shutdown=settings.GRACEFUL_SHUTDOWN_SECONDS,  # Không treo vô hạn khi tắt.
        proxy_headers=True,                               # Lấy IP thật của client khi chạy sau proxy.
        forwarded_allow_ips="*",                          # Production: thay "*" bằng IP của proxy.
    )
