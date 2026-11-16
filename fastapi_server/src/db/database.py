from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from urllib.parse import quote_plus
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from config.config import settings

class Base(DeclarativeBase):
    """
    Base class cho toàn bộ model SQLAlchemy.

    Tất cả bảng ORM như Users, Roles, UserAuth sẽ kế thừa từ Base.
    Khi gọi Base.metadata.create_all(), SQLAlchemy sẽ tạo các bảng này.
    """


def build_database_url() -> str:
    """
    Tạo chuỗi kết nối SQL Server async.
    """
    database_url = settings.database_url
    if database_url:
        return database_url

    # Nếu DATABASE_URL không có, tạo chuỗi kết nối từ các biến môi trường khác.
    connection_string = (
        f"DRIVER={settings.sql_server_driver};"
        f"SERVER={settings.db_host};"
        f"DATABASE={settings.db_name};"
        f"UID={settings.db_user};"
        f"PWD={settings.db_password};"
        # f"Encrypt={os.getenv('SQL_SERVER_ENCRYPT', 'yes')};"
        f"TrustServerCertificate={settings.trust_db_server};")

    # Trả về chuỗi kết nối theo định dạng mà SQLAlchemy async sử dụng
    return f"mssql+aioodbc:///?odbc_connect={quote_plus(connection_string)}"

# Lấy chuỗi kết nối khi module được import
DATABASE_URL = build_database_url()

# Tạo Async Engine để kết nối với SQL Server
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=int(settings.db_pool),
    max_overflow=int(settings.db_max_overflow),
    # pyodbc gửi cả mảng tham số trong 1 lần cho executemany (INSERT/UPDATE hàng loạt)
    # thay vì từng dòng một. Quan trọng khi 1 thông báo gửi tới hàng nghìn người.
    fast_executemany=True,
    # Chờ tối đa N giây để lấy connection khi pool đã hết (mặc định SQLAlchemy 30 giây).
    pool_timeout=float(os.getenv("DB_POOL_TIMEOUT", "30")),
)

# ------------------------------------------------------------
# AN TOÀN KHI CHỊU TẢI
# ------------------------------------------------------------
# 1) LOCK_TIMEOUT: câu lệnh chờ khóa quá N ms sẽ báo lỗi (1222) thay vì chờ VÔ HẠN.
#    Transaction lỗi sẽ rollback, outbox quay về pending/được recover => không mất thông báo,
#    nhưng không bao giờ làm treo cả hệ thống. 0 hoặc âm = giữ mặc định của SQL Server (chờ mãi).
DB_LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "10000"))


@event.listens_for(engine.sync_engine, "connect")
def _set_session_options(dbapi_connection, _connection_record) -> None:
    if DB_LOCK_TIMEOUT_MS > 0:
        cursor = dbapi_connection.cursor()
        cursor.execute(f"SET LOCK_TIMEOUT {DB_LOCK_TIMEOUT_MS}")
        cursor.close()


# 2) Thread pool cho aioodbc: aioodbc chạy MỌI lệnh pyodbc (execute, fetch, commit...) trong
#    thread pool mặc định của event loop (chỉ ~min(32, số CPU + 4) thread). Nếu số connection
#    trong pool > số thread, khi nhiều lệnh cùng chờ khóa thì các phiên đang GIỮ khóa không còn
#    thread để chạy COMMIT => treo vĩnh viễn (SQL Server không phát hiện được vì bên chặn là client).
#    => Số thread phải >= số connection tối đa của pool.
DB_EXECUTOR_THREADS = int(
    os.getenv(
        "DB_EXECUTOR_THREADS",
        str(int(settings.db_pool) + int(settings.db_max_overflow) + 8),
    )
)


def configure_db_executor(loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Gọi 1 lần khi khởi động (lifespan của API, main() của worker), trong event loop đang chạy."""
    loop = loop or asyncio.get_running_loop()
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=DB_EXECUTOR_THREADS, thread_name_prefix="db")
    )


# Factory tạo session async. Mỗi request FastAPI sẽ có một session riêng.
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

async def get_db():
    """
    Dependency lấy DB session cho FastAPI async.

    Cách dùng:

        async def endpoint(db: AsyncSession = Depends(get_db)):
            ...

    Mỗi request sẽ có một session riêng.
    Sau khi request kết thúc, session được đóng tự động.
    """
    async with AsyncSessionLocal() as db:
        yield db

async def init_db() -> None:
    """
    Tạo bảng nếu chưa có.

    Demo có thể dùng create_all().
    Production nên dùng Alembic migration để quản lý thay đổi schema tốt hơn.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def close_db() -> None:
    """
    Đóng connection pool khi service shutdown.
    """
    await engine.dispose()
