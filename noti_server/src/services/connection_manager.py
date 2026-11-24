"""
Quản lý toàn bộ kết nối WebSocket đang mở trên process hiện tại.

Nhiệm vụ:
1. GIỚI HẠN TẢI (admission control):
   - Tổng số kết nối <= WS_MAX_CONNECTIONS  -> vượt thì đóng 1013 (Try Again Later).
   - Số kết nối của 1 user <= WS_MAX_CONNECTIONS_PER_USER -> vượt thì đóng 4429.
   Nhờ Stream Hub, WebSocket không còn giữ kết nối Redis riêng; giới hạn ở đây
   bảo vệ RAM/CPU/file descriptor của process và chặn 1 user mở quá nhiều kết nối.

2. QUAN SÁT (observability): đếm số kết nối hiện tại, tổng số đã phục vụ,
   số event đã gửi, số lần bị từ chối... để xem qua endpoint /health/stats.

3. TẮT AN TOÀN (graceful shutdown): khi server tắt (deploy bản mới), chủ động
   đóng mọi kết nối với code 1012 (Service Restart) để client biết mà reconnect
   ngay vào instance khác, thay vì chờ timeout.

Lưu ý khi chạy nhiều process/instance (uvicorn --workers N hoặc nhiều container):
mỗi process có ConnectionManager riêng, giới hạn tính theo TỪNG process.
Muốn giới hạn toàn cụm thì cần đếm trong Redis (để dành khi thật sự cần).
"""
from __future__ import annotations
import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from config.configs import settings
from utils import close_codes
from utils.auth import Identity

logger = logging.getLogger("noti.connections")


class ConnectionRejected(Exception):
    """Không nhận thêm kết nối. code/reason dùng để đóng WebSocket."""

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


@dataclass
class ConnectionInfo:
    """Thông tin của 1 kết nối đang mở."""

    connection_id: str
    websocket: WebSocket
    identity: Identity
    channel: str
    stream_key: str
    connected_at: float = field(default_factory=time.monotonic)
    events_sent: int = 0


class ConnectionManager:
    """
    Quản lý kết nối WebSocket đang mở.
    """
    def __init__(self, *, max_connections: int, max_per_user: int) -> None:
        self._max_connections = max_connections
        self._max_per_user = max_per_user

        # connection_id -> ConnectionInfo
        self._connections: dict[str, ConnectionInfo] = {}
        # (tenant_id, user_id) -> số kết nối hiện có
        self._per_user: Counter[tuple[str, str]] = Counter()

        # asyncio chạy 1 luồng nhưng giữa các `await` vẫn có thể xen kẽ,
        # dùng Lock để thao tác "kiểm tra rồi thêm" là nguyên tử.
        self._lock = asyncio.Lock()
        self._shutting_down = False

        # Bộ đếm cộng dồn từ lúc khởi động (dùng cho giám sát).
        self._stats: Counter[str] = Counter()
        self._started_at = time.time()

    # ------------------------------------------------------------------
    # Đăng ký / hủy đăng ký
    # ------------------------------------------------------------------
    async def register(self, info: ConnectionInfo) -> None:
        """
        Đăng ký kết nối mới. Ném ConnectionRejected nếu vượt giới hạn.

        Gọi SAU khi xác thực thành công (cần identity để đếm theo user).
        """
        # Khóa định danh user để đếm số kết nối hiện có của user này.
        user_key = (info.identity.tenant_id, info.identity.user_id)

        async with self._lock:
            # Server đang tắt, không nhận thêm kết nối mới. Client nên reconnect vào instance khác.
            if self._shutting_down:
                self._stats["rejected_shutting_down"] += 1
                raise ConnectionRejected(close_codes.SERVICE_RESTART, "server_shutting_down")

            # Process đang quá tải, không nhận thêm kết nối mới. Client nên thử lại sau vài giây.
            if len(self._connections) >= self._max_connections:
                self._stats["rejected_server_full"] += 1
                raise ConnectionRejected(close_codes.TRY_AGAIN_LATER, "server_at_capacity")

            # User này đang mở quá nhiều kết nối, không nhận thêm. Client nên thử lại sau vài giây.
            if self._per_user[user_key] >= self._max_per_user:
                self._stats["rejected_user_limit"] += 1
                raise ConnectionRejected(close_codes.TOO_MANY_CONNECTIONS, "too_many_connections_for_user")

            self._connections[info.connection_id] = info
            self._per_user[user_key] += 1
            self._stats["total_accepted"] += 1

    async def unregister(self, connection_id: str) -> ConnectionInfo | None:
        """
        Hủy đăng ký. An toàn khi gọi nhiều lần (lần sau không làm gì).
        """
        async with self._lock:
            # Lấy thông tin kết nối, xóa khỏi dict
            info = self._connections.pop(connection_id, None)
            if info is None:
                return None

            # Khóa định danh user để giảm số kết nối hiện có của user này.
            user_key = (info.identity.tenant_id, info.identity.user_id)
            self._per_user[user_key] -= 1
            if self._per_user[user_key] <= 0:
                del self._per_user[user_key]  # Xóa để Counter không phình to theo thời gian.

            self._stats["total_events_sent"] += info.events_sent
            return info

    def record_rejection(self, reason: str) -> None:
        """
        Ghi nhận kết nối bị từ chối ở bước xác thực (để thống kê).
        """
        self._stats[f"rejected_{reason}"] += 1

    # ------------------------------------------------------------------
    # Thống kê
    # ------------------------------------------------------------------
    @property
    def active_count(self) -> int:
        """Số kết nối đang mở."""
        return len(self._connections)

    @property
    def is_shutting_down(self) -> bool:
        """Server đang tắt, không nhận thêm kết nối mới."""
        return self._shutting_down

    def snapshot(self) -> dict[str, Any]:
        """Số liệu hiện tại, trả về qua /health/stats."""
        by_channel = Counter(c.channel for c in self._connections.values())
        live_events = sum(c.events_sent for c in self._connections.values())
        stats = dict(self._stats)
        stats["total_events_sent"] = stats.get("total_events_sent", 0) + live_events

        return {
            "active_connections": self.active_count,
            "active_users": len(self._per_user),
            "active_by_channel": dict(by_channel),
            "max_connections": self._max_connections,
            "max_connections_per_user": self._max_per_user,
            "uptime_seconds": int(time.time() - self._started_at),
            "shutting_down": self._shutting_down,
            "counters": stats,
        }

    # ------------------------------------------------------------------
    # Tắt server
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        """
        Đóng mọi kết nối còn lại với code 1012 khi server tắt.

        Sau khi gọi, mọi kết nối mới đều bị từ chối.
        """
        async with self._lock:
            self._shutting_down = True
            connections = list(self._connections.values())

        if not connections:
            return

        logger.info("Closing websocket connections for shutdown", extra={"count": len(connections)})

        async def _close(info: ConnectionInfo) -> None:
            try:
                if info.websocket.application_state == WebSocketState.CONNECTED:
                    await info.websocket.close(code=close_codes.SERVICE_RESTART, reason="server_restarting")
            except Exception:  # pylint: disable=broad-except
                pass

        # Đóng song song, có giới hạn thời gian để không treo quá trình tắt server.
        await asyncio.wait(
            [asyncio.create_task(_close(c)) for c in connections],
            timeout=5,
        )


# Instance dùng chung cho toàn bộ app (1 process = 1 manager).
connection_manager = ConnectionManager(
    max_connections=settings.WS_MAX_CONNECTIONS,
    max_per_user=settings.WS_MAX_CONNECTIONS_PER_USER,
)
