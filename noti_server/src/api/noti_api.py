"""
Các endpoint WebSocket nhận thông báo realtime.

┌──────────────────────────────┬──────────────────┬───────────────────────────────┬─────────────────────────────┐
│ Đường dẫn                    │ channel (ticket) │ Quyền cần có                  │ Redis Stream                │
├──────────────────────────────┼──────────────────┼───────────────────────────────┼─────────────────────────────┤
│ /noti/ws/events              │ user             │ notification:subscribe        │ ...:tenant:{t}:user:{u}     │
│ /noti/ws/tenant-dashboard    │ tenant_dashboard │ notification:tenant_dashboard │ ...:tenant:{t}:dashboard    │
│ /noti/ws/jobs/{job_id}       │ job              │ notification:job_watch        │ ...:tenant:{t}:job:{job_id} │
│ /noti/ws/global-events       │ global           │ notification:global_subscribe │ stream:noti:global          │
└──────────────────────────────┴──────────────────┴───────────────────────────────┴─────────────────────────────┘

Kết nối mẫu:
    ws://127.0.0.1:1234/noti/ws/events?ticket=<WS_TICKET>&last_event_id=<ID_CUỐI_ĐÃ_NHẬN>
"""

from __future__ import annotations

import logging
import re
from typing import Callable

from fastapi import APIRouter, WebSocket

from controllers.noti_controller import NotiController
from services.connection_manager import connection_manager
from utils import close_codes
from utils.auth import Identity, WebSocketAuthError, authenticate_websocket
from utils.redis_keys import global_stream_key, job_stream_key, tenant_stream_key, user_stream_key

logger = logging.getLogger("noti.api")

router = APIRouter(
    prefix="/noti",
    tags=["Notifications"]
)

# job_id: tối đa 200 ký tự, không chứa khoảng trắng / ký tự điều khiển.
# (Bảo vệ chính vẫn là ticket phải được cấp cho đúng job_id này.)
_JOB_ID_RE = re.compile(r"^[^\s\x00-\x1f\x7f]{1,200}$")


async def _serve_channel(
    websocket: WebSocket,
    *,
    channel: str,
    permission: str,
    stream_key_of: Callable[[Identity], str],
    job_id: str | None = None,
) -> None:
    """
    Luồng xử lý WebSocket cho 1 channel cụ thể.

    1. accept()
    2. Xác thực ticket + kiểm tra channel + quyền
    3. Tính stream key từ identity (KHÔNG lấy từ input của client -> không thể
       nghe trộm stream của tenant/user khác)
    4. Giao cho controller stream event
    """
    # Chấp nhận kết nối WebSocket trước khi xác thực để client nhận được lỗi 403/401.
    await websocket.accept()

    try:
        # Xác thực ticket, kiểm tra channel, quyền, job_id (nếu có).
        identity = await authenticate_websocket(
            websocket,
            expected_channel=channel,
            required_permission=permission,
            job_id=job_id,
        )
    except WebSocketAuthError as exc:
        connection_manager.record_rejection(exc.reason)
        logger.info(
            "Không thể xác thực WebSocket",
            extra={"channel": channel, "reason": exc.reason, "client": _client_addr(websocket)},
        )
        await NotiController.safe_close(websocket, exc.code, exc.reason)
        return

    # Kết nối thành công, giao cho controller stream event.
    await NotiController.run_stream_websocket(
        websocket=websocket,
        identity=identity,
        stream_key=stream_key_of(identity),
        channel=channel,
    )


def _client_addr(websocket: WebSocket) -> str | None:
    """
    Lấy địa chỉ IP:port của client, nếu có.
    """
    return f"{websocket.client.host}:{websocket.client.port}" if websocket.client else None


@router.websocket("/ws/events")
async def websocket_user_events(websocket: WebSocket) -> None:
    """
    Nhận event của CHÍNH user đang đăng nhập (endpoint chính cho người dùng).
    """
    await _serve_channel(
        websocket,
        channel="user",
        permission="notification:subscribe",
        stream_key_of=lambda ident: user_stream_key(ident.tenant_id, ident.user_id),
    )


@router.websocket("/ws/tenant-dashboard")
async def websocket_tenant_dashboard(websocket: WebSocket) -> None:
    """
    Admin theo dõi toàn bộ event trong tenant của mình.
    Dành cho màn hình dashboard của tenant (vd màn hình tổng quan job).
    """
    await _serve_channel(
        websocket,
        channel="tenant_dashboard",
        permission="notification:tenant_dashboard",
        stream_key_of=lambda ident: tenant_stream_key(ident.tenant_id),
    )


@router.websocket("/ws/jobs/{job_id}")
async def websocket_job_events(websocket: WebSocket, job_id: str) -> None:
    """
    Nhận event của MỘT job (vd màn hình chi tiết job).

    Ticket phải được API server cấp với đúng job_id này (API chịu trách nhiệm
    kiểm tra job có thuộc về user hay không trước khi cấp ticket).
    """
    if not _JOB_ID_RE.match(job_id):
        await websocket.accept()
        await NotiController.safe_close(websocket, close_codes.BAD_REQUEST, "invalid_job_id")
        return

    await _serve_channel(
        websocket,
        channel="job",
        permission="notification:job_watch",
        stream_key_of=lambda ident: job_stream_key(ident.tenant_id, job_id),
        job_id=job_id,
    )


@router.websocket("/ws/global-events")
async def websocket_global_events(websocket: WebSocket) -> None:
    """
    Event toàn hệ thống, dành cho superuser/đội vận hành (monitoring).
    Ticket phải được API server cấp với channel=global và quyền notification:global_subscribe.
    """
    await _serve_channel(
        websocket,
        channel="global",
        permission="notification:global_subscribe",
        stream_key_of=lambda _ident: global_stream_key(),
    )
