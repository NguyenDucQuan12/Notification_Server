from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Cấu hình dùng chung cho API service, worker service và notification service.

    Nên đưa các giá trị này vào file .env trong môi trường thật.
    """

    # Đường dẫn kết nối tới Redis, Redis này phải khớp với Redis mà notification, API, worker cùng dùng để publish/subscribe event.
    REDIS_URL: str = "redis://localhost:6379/0"

    # JWT secret key và algorithm để decode token, phải khớp với JWT mà API service tạo ra.
    JWT_SECRET_KEY: str = "CHANGE_THIS_SECRET_KEY"
    JWT_ALGORITHM: str = "HS256"

    # Số event đọc mỗi batch khi replay event cũ.
    REPLAY_BATCH_SIZE: int = 100

    # Thời gian XREAD BLOCK đợi event mới, đơn vị milliseconds.
    # Nếu hết thời gian mà không có event mới thì gửi heartbeat.
    STREAM_BLOCK_MS: int = 15000

    # Giới hạn số event giữ trong stream.
    # Dùng để tránh Redis Stream phình quá lớn.
    STREAM_MAXLEN: int = 10000


settings = Settings()