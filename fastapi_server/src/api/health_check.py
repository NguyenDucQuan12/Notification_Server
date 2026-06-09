from fastapi import APIRouter
from fastapi.responses import JSONResponse
from db.database import engine
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import text
import os

from services.redis_client import get_redis_fast


load_dotenv()  # Tự động tìm và nạp file .env ở thư mục hiện tại

UPLOAD_DIRECTORY = os.getenv("UPLOAD_DIRECTORY", "/app/uploads")

# Khai báo router với tiền tố cho các endpoint là: /user_login/xxx
router = APIRouter(
    tags= ["Health"]
)


@router.get("/healthz", summary="Liveness probe")
async def healthz():
    """
    Kiểm tra sống/chết cơ bản của tiến trình.
    """
    return {"status": "ok"}

@router.get("/readyz", summary="Readiness probe")
async def readyz():
    """
    Kiểm tra service đã sẵn sàng nhận request chưa.

    Kiểm tra:
    1. DB:
       - kết nối SQL Server bằng async engine
       - chạy SELECT 1

    2. Redis:
       - ping Redis
       - nếu ping trả True/PONG thì ok

    3. File system:
       - tạo thư mục upload nếu chưa có
       - thử ghi file tạm
       - xóa file tạm

    Trả về:
    - 200 nếu tất cả ok
    - 503 nếu một trong các check lỗi
    """

    checks: dict[str, str] = {}

    # ======================================================
    # 1. Kiểm tra DB
    # ======================================================
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            value = result.scalar_one()

        if value == 1:
            checks["db"] = "ok"
        else:
            checks["db"] = f"error: unexpected result {value}"

    except Exception as exc:
        checks["db"] = f"error: {type(exc).__name__}: {str(exc)}"

    # ======================================================
    # 2. Kiểm tra Redis
    # ======================================================
    try:
        redis_client = get_redis_fast()

        pong = await redis_client.ping()

        if pong is True or pong == "PONG":
            checks["redis"] = "ok"
        else:
            checks["redis"] = f"error: unexpected ping result {pong}"

    except Exception as exc:
        checks["redis"] = f"error: {type(exc).__name__}: {str(exc)}"

    # ======================================================
    # 3. Kiểm tra quyền ghi thư mục upload
    # ======================================================
    try:
        upload_dir = Path(UPLOAD_DIRECTORY)
        upload_dir.mkdir(parents=True, exist_ok=True)

        probe_file = upload_dir / ".readyz.tmp"

        probe_file.write_text(
            "ok",
            encoding="utf-8",
        )

        probe_file.unlink(missing_ok=True)

        checks["fs"] = "ok"

    except Exception as exc:
        checks["fs"] = f"error: {type(exc).__name__}: {str(exc)}"

    # ======================================================
    # 4. Tổng hợp kết quả
    # ======================================================
    ok = all(value == "ok" for value in checks.values())

    return JSONResponse(
        status_code=200 if ok else 503,
        content={
            "status": "ok" if ok else "error",
            "checks": checks,
        },
    )