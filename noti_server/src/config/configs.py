"""
Cấu hình tập trung của Notification Server.

Nguyên tắc (theo mô hình 12-factor app mà các doanh nghiệp hay dùng):
- MỌI tham số có thể thay đổi giữa các môi trường (dev / staging / production)
  đều đọc từ biến môi trường, KHÔNG viết cứng trong code.
- Mỗi tham số có giá trị mặc định an toàn để chạy local ngay mà không cần .env.
- Cấu hình được kiểm tra (validate) ngay khi khởi động. Nếu sai thì server
  dừng luôn với thông báo rõ ràng, thay vì chạy rồi mới lỗi lúc có client kết nối.

Thứ tự ưu tiên khi đọc giá trị (cao -> thấp):
1. Biến môi trường thật của hệ điều hành / Docker (env_file trong docker-compose).
2. Tệp .env đặt ở thư mục chạy lệnh (nếu có).
3. Giá trị mặc định khai báo bên dưới.

Xem tệp `noti_server/.env.example` để biết danh sách đầy đủ.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Thư mục gốc dự án (Notification_Server). Chạy local: <repo>/env/... ;
# trong Docker các tệp này không tồn tại -> pydantic-settings tự bỏ qua.
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Tệp đứng SAU ghi đè tệp đứng TRƯỚC; biến môi trường thật luôn ưu tiên cao nhất.
        env_file=(
            _PROJECT_ROOT / "env" / ".env.common",   # Dùng chung mọi service (REDIS_URL...)
            _PROJECT_ROOT / "env" / ".env.noti",     # Riêng Notification Server
            ".env",                                  # Tùy chọn: .env ở thư mục đang chạy lệnh
        ),
        env_file_encoding="utf-8",
        extra="ignore",             # Bỏ qua biến lạ, tránh lỗi khi dùng chung tệp env với service khác.
        case_sensitive=False,
    )

    # ------------------------------------------------------------------
    # 1. Thông tin ứng dụng
    # ------------------------------------------------------------------
    APP_NAME: str = "Notification Service"
    APP_VERSION: str = "2.0.0"

    # Môi trường đang chạy. Ở "production" sẽ tự tắt /docs và bật log JSON.
    ENVIRONMENT: Literal["development", "staging", "production"] = "development"

    HOST: str = "0.0.0.0"
    # Giữ 1234 để tương thích với client hiện tại (client/listen_noti_server.py).
    PORT: int = 1234

    # Dùng khi chạy sau reverse proxy (Nginx/Traefik) có tiền tố đường dẫn, ví dụ "/quan".
    # LƯU Ý: root_path KHÔNG đổi đường dẫn route trong app; proxy phải cắt tiền tố
    # trước khi chuyển request vào. Nó chỉ giúp Swagger /docs sinh URL đúng.
    ROOT_PATH: str = ""

    # Bật/tắt trang Swagger. None = tự quyết theo ENVIRONMENT (tắt ở production).
    ENABLE_DOCS: bool | None = None

    # ------------------------------------------------------------------
    # 2. Logging
    # ------------------------------------------------------------------
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    # Log dạng JSON (mỗi dòng 1 object) để đẩy vào ELK/Loki/Datadog.
    # None = tự quyết theo ENVIRONMENT (bật ở production).
    LOG_JSON: bool | None = None

    # ------------------------------------------------------------------
    # 3. Redis
    # ------------------------------------------------------------------
    # Phải trỏ tới CÙNG một Redis mà API server và worker đang XADD event vào.
    # Chạy local: redis://127.0.0.1:6379/0 ; trong docker-compose: redis://redis:6379/0
    REDIS_URL: str = "redis://127.0.0.1:6379/0"

    # Timeout khi mở kết nối TCP tới Redis (giây).
    REDIS_CONNECT_TIMEOUT: float = 2.0

    # Timeout cho lệnh nhanh (GETDEL ticket, PING, XRANGE...) (giây).
    REDIS_SOCKET_TIMEOUT: float = 3.0

    # Số kết nối tối đa của pool "fast" (GETDEL ticket, XREVRANGE khi replay, PING).
    # Dùng BlockingConnectionPool: khi hết kết nối, request CHỜ (tối đa REDIS_POOL_TIMEOUT)
    # thay vì lỗi ngay. Quan trọng khi hàng nghìn client reconnect cùng lúc (vd sau deploy).
    REDIS_MAX_CONNECTIONS: int = 50
    REDIS_POOL_TIMEOUT: float = 5.0

    # Timeout socket cho pool "stream" (XREAD BLOCK của Stream Hub).
    # BẮT BUỘC > HUB_BLOCK_MS/1000 + 1, nếu không redis-py báo TimeoutError trước khi Redis kịp trả lời.
    REDIS_STREAM_SOCKET_TIMEOUT: float = 10.0

    # ------------------------------------------------------------------
    # 4. Stream Hub (đọc Redis Stream dùng chung cho mọi WebSocket)
    # ------------------------------------------------------------------
    # Số "shard" đọc song song. Mỗi shard = 1 task + 1 kết nối Redis, phụ trách
    # một phần các stream (chia theo hash của stream key).
    # 10.000 client ~ 10.000 stream -> mỗi shard XREAD ~2.500 stream trong 1 lệnh.
    HUB_SHARDS: int = Field(default=4, ge=1, le=64)

    # Thời gian 1 lệnh XREAD BLOCK của shard chờ event (ms).
    # Ngắn để stream MỚI đăng ký được đưa vào lệnh XREAD kế tiếp nhanh
    # (độ trễ tối đa của event live ĐẦU TIÊN với stream vừa đăng ký = giá trị này).
    HUB_BLOCK_MS: int = Field(default=1000, ge=100, le=30000)

    # Số event tối đa đọc từ MỖI stream trong 1 lệnh XREAD.
    STREAM_READ_COUNT: int = Field(default=100, ge=1, le=1000)

    # Hàng đợi riêng của mỗi WebSocket (số event). Client nhận chậm làm hàng đợi
    # đầy => bị ngắt (4408) thay vì để bộ nhớ server phình to vô hạn.
    HUB_SUBSCRIBER_QUEUE_SIZE: int = Field(default=1000, ge=10)

    # Không có event trong khoảng này (giây) thì gửi heartbeat cho client.
    HEARTBEAT_INTERVAL_SECONDS: float = Field(default=15.0, ge=1.0, le=120.0)

    # Số event cũ TỐI ĐA được replay khi client kết nối lại.
    # Nếu client bỏ lỡ nhiều hơn, server chỉ gửi N event mới nhất và gửi kèm
    # message "replay_incomplete" để client tự đồng bộ lại qua REST API.
    # Tránh trường hợp 1 client mới kết nối bị dội 10.000 event một lúc.
    REPLAY_MAX_EVENTS: int = Field(default=1000, ge=0, le=10000)

    # ------------------------------------------------------------------
    # 5. WebSocket
    # ------------------------------------------------------------------
    # Tổng số WebSocket tối đa trên MỖI process. Vượt quá sẽ từ chối với code 1013
    # (Try Again Later). Nhờ Stream Hub, số WebSocket KHÔNG còn bị giới hạn bởi số
    # kết nối Redis; giới hạn thực tế là RAM/CPU và ulimit (số file descriptor) của OS.
    WS_MAX_CONNECTIONS: int = Field(default=10000, ge=1)

    # Số WebSocket tối đa của MỘT user trên mỗi process (ví dụ mở nhiều tab).
    # Chống 1 tài khoản (hoặc script lỗi) chiếm hết tài nguyên.
    WS_MAX_CONNECTIONS_PER_USER: int = Field(default=10, ge=1)

    # Danh sách Origin được phép mở WebSocket, phân tách bằng dấu phẩy.
    # Chống tấn công Cross-Site WebSocket Hijacking (CSWSH).
    # Để trống = không kiểm tra (client không phải trình duyệt thường không gửi Origin).
    # Ví dụ: "https://app.example.com,http://localhost:3000"
    WS_ALLOWED_ORIGINS: str = ""

    # Thời gian tối đa (giây) để gửi xong 1 message tới client.
    # Client mạng quá chậm (slow consumer) sẽ bị ngắt để không giữ bộ nhớ server.
    WS_SEND_TIMEOUT_SECONDS: float = Field(default=10.0, gt=0)

    # Kích thước tối đa 1 message client được gửi lên (byte). Client chỉ cần gửi "ping".
    WS_MAX_MESSAGE_SIZE: int = 64 * 1024

    # Ping ở tầng giao thức WebSocket do uvicorn tự gửi (giây), giúp phát hiện kết nối chết.
    WS_PING_INTERVAL: float = 20.0
    WS_PING_TIMEOUT: float = 20.0

    # Thời gian tối đa chờ các kết nối đóng khi tắt server (giây).
    GRACEFUL_SHUTDOWN_SECONDS: int = 10

    # ------------------------------------------------------------------
    # 6. CORS (chỉ áp dụng cho HTTP như /health, KHÔNG áp dụng cho WebSocket)
    # ------------------------------------------------------------------
    CORS_ALLOWED_ORIGINS: str = "http://localhost:3000,http://172.31.99.130"

    # ------------------------------------------------------------------
    # Thuộc tính tính toán (không đọc từ env)
    # ------------------------------------------------------------------
    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT == "production"

    @property
    def docs_enabled(self) -> bool:
        return (not self.is_production) if self.ENABLE_DOCS is None else self.ENABLE_DOCS

    @property
    def log_json(self) -> bool:
        return self.is_production if self.LOG_JSON is None else self.LOG_JSON

    @property
    def ws_allowed_origins(self) -> set[str]:
        return _split_csv(self.WS_ALLOWED_ORIGINS)

    @property
    def cors_allowed_origins(self) -> list[str]:
        return sorted(_split_csv(self.CORS_ALLOWED_ORIGINS))

    @property
    def redis_stream_max_connections(self) -> int:
        # Mỗi shard giữ 1 kết nối + dự phòng 2.
        return self.HUB_SHARDS + 2

    # ------------------------------------------------------------------
    # Kiểm tra chéo giữa các tham số
    # ------------------------------------------------------------------
    @model_validator(mode="after")
    def _validate_consistency(self) -> "Settings":
        block_seconds = self.HUB_BLOCK_MS / 1000

        # Lỗi rất hay gặp: socket_timeout <= thời gian BLOCK => mỗi lần không có
        # event, redis-py ném TimeoutError và shard phải kết nối lại liên tục.
        if self.REDIS_STREAM_SOCKET_TIMEOUT <= block_seconds + 1:
            raise ValueError(
                "REDIS_STREAM_SOCKET_TIMEOUT phải lớn hơn HUB_BLOCK_MS/1000 + 1 giây "
                f"(hiện tại {self.REDIS_STREAM_SOCKET_TIMEOUT}s <= {block_seconds + 1}s)"
            )

        if self.WS_MAX_CONNECTIONS_PER_USER > self.WS_MAX_CONNECTIONS:
            raise ValueError("WS_MAX_CONNECTIONS_PER_USER không được lớn hơn WS_MAX_CONNECTIONS")

        return self


def _split_csv(value: str) -> set[str]:
    """Tách chuỗi "a, b ,c" thành {"a", "b", "c"}, bỏ phần tử rỗng và dấu "/" cuối."""
    return {item.strip().rstrip("/") for item in value.split(",") if item.strip()}


# Khởi tạo 1 lần duy nhất khi import. Nếu cấu hình sai sẽ lỗi ngay tại đây.
settings = Settings()
