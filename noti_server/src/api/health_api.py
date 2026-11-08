"""
Các endpoint kiểm tra sức khỏe

- GET /health/live  (liveness) : process còn chạy không?
    Không kiểm tra Redis. Nếu endpoint này lỗi, orchestrator sẽ KHỞI ĐỘNG LẠI container.
    (Không nên restart chỉ vì Redis chết — restart cũng không làm Redis sống lại.)

- GET /health/ready (readiness): sẵn sàng nhận kết nối mới chưa?
    Kiểm tra Redis + server không đang tắt. Nếu lỗi (HTTP 503), load balancer
    tạm NGỪNG chuyển client mới vào instance này, nhưng không restart nó.

- GET /health/stats : số liệu vận hành (số kết nối, số event đã gửi...).
    Nên chặn endpoint này ở proxy/firewall, chỉ cho mạng nội bộ truy cập.
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from config.configs import settings
from services.connection_manager import connection_manager
from services.redis_client import ping_redis
from utils.utils import utc_now_iso

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/live")
async def liveness() -> dict[str, str]:
    """
    Endpoint kiểm tra liveness (process còn chạy không?).
    Không kiểm tra Redis. Nếu endpoint này lỗi, orchestrator sẽ KHỞI ĐỘNG LẠI container.
    (Không nên restart chỉ vì Redis chết — restart cũng không làm Redis sống lại.)
    """
    return {"status": "alive", "server_time": utc_now_iso()}


@router.get("/ready")
async def readiness() -> JSONResponse:
    """
    Endpoint kiểm tra readiness (sẵn sàng nhận kết nối mới chưa?).
    Kiểm tra Redis + server không đang tắt. Nếu lỗi (HTTP 503), load balancer tạm NGỪNG chuyển client mới vào instance này, nhưng không restart nó.
    """
    redis_ok = await ping_redis()
    ready = redis_ok and not connection_manager.is_shutting_down

    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ready" if ready else "not_ready",
            "checks": {
                "redis": "ok" if redis_ok else "unavailable",
                "shutting_down": connection_manager.is_shutting_down,
            },
            "active_connections": connection_manager.active_count,
            "server_time": utc_now_iso(),
        },
    )


@router.get("/stats")
async def stats() -> dict:
    """
    Endpoint trả số liệu vận hành (số kết nối, số event đã gửi...).
    Nên chặn endpoint này ở proxy/firewall, chỉ cho mạng nội bộ truy cập.
    """
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT,
        **connection_manager.snapshot(),
        "server_time": utc_now_iso(),
    }
