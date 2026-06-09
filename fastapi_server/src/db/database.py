from __future__ import annotations

import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


load_dotenv()


class Base(DeclarativeBase):
    """
    Base class cho toàn bộ model SQLAlchemy.

    Tất cả bảng ORM như Users, Roles, UserAuth sẽ kế thừa từ Base.
    Khi gọi Base.metadata.create_all(), SQLAlchemy sẽ tạo các bảng này.
    """
    pass


def build_database_url() -> str:
    """
    Tạo chuỗi kết nối SQL Server async.
    """
    # Lấy DATABASE_URL nếu đã được khai báo trong biến môi trường, thường dùng trong môi trường production với các dịch vụ như Azure SQL.
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    # Nếu DATABASE_URL không có, tạo chuỗi kết nối từ các biến môi trường khác.
    connection_string = (
        f"DRIVER={{{os.getenv('SQL_SERVER_DRIVER', 'ODBC Driver 17 for SQL Server')}}};"
        f"SERVER={os.getenv('SQL_SERVER_HOST', 'localhost')};"
        f"DATABASE={os.getenv('SQL_SERVER_DATABASE')};"
        f"UID={os.getenv('SQL_SERVER_USERNAME')};"
        f"PWD={os.getenv('SQL_SERVER_PASSWORD')};"
        # f"Encrypt={os.getenv('SQL_SERVER_ENCRYPT', 'yes')};"
        f"TrustServerCertificate={os.getenv('SQL_SERVER_TRUST_CERTIFICATE', 'yes')};")

    # Trả về chuỗi kết nối theo định dạng mà SQLAlchemy async cần, sử dụng quote_plus để mã hóa connection string cho URL.
    return f"mssql+aioodbc:///?odbc_connect={quote_plus(connection_string)}"

# Lấy chuỗi kết nối khi module được import. Điều này giúp tránh việc phải gọi build_database_url() nhiều lần khi tạo engine hoặc session.
DATABASE_URL = build_database_url()

# Tạo Async Engine để kết nối với SQL Server. Các tham số pool_pre_ping, pool_recycle, pool_size, max_overflow giúp quản lý kết nối hiệu quả hơn.
engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("DB_ECHO", "false").lower() == "true",
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
)

# Factory tạo session async. Mỗi request hoặc task nên sử dụng một session riêng để tránh xung đột dữ liệu. expire_on_commit=False giúp các object vẫn có thể truy cập được các field sau khi commit.
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