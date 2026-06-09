from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status

from auth.oauth2 import required_token_user
from services.ws_ticket_service import create_ws_ticket


router = APIRouter(
    prefix="/notifications",
    tags=["Notifications"],
)


def has_permission(current_user: dict[str, Any], permission: str) -> bool:
    """
    Kiểm tra permission của user.

    Superuser được qua toàn bộ.
    """

    if current_user.get("is_superuser"):
        return True

    permissions = current_user.get("permissions") or []

    return permission in permissions


@router.post("/ws-ticket")
async def create_notification_ws_ticket(
    channel: Literal["user", "tenant_dashboard", "job", "global"] = Query(...),
    job_id: str | None = Query(default=None),
    current_user: dict[str, Any] = Depends(required_token_user),
) -> dict[str, Any]:
    """
    API cấp WebSocket ticket.
    Ticket này dùng để client mở WebSocket và subscribe vào stream tương ứng, nó chỉ sống rất ngắn và dùng một lần và được lưu trong Redis để notification service xác thực khi client kết nối WebSocket.

    Ticket chỉ có nhiệm vụ xác thực mở kết nối WebSocket ban đầu, sau khi kết nối WebSocket đã mở thành công thì ticket này sẽ bị xóa khỏi Redis, và client sẽ nhận event qua kết nối WebSocket mà không cần gửi token nữa.
    Client gọi API này trước khi mở WebSocket.

    Ví dụ:
    POST /notifications/ws-ticket?channel=user

    hoặc:
    POST /notifications/ws-ticket?channel=tenant_dashboard

    hoặc:
    POST /notifications/ws-ticket?channel=job&job_id=<JOB_ID>

    hoặc:
    POST /notifications/ws-ticket?channel=global
    """

    if channel == "user":
        required_permission = "notification:subscribe"

    elif channel == "tenant_dashboard":
        required_permission = "notification:tenant_dashboard"

    elif channel == "global":
        required_permission = "notification:global_subscribe"

    else:
        required_permission = "notification:job_watch"

        if not job_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "job_id là bắt buộc khi channel=job"},
            )

    if not has_permission(current_user, required_permission):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"message": f"Thiếu quyền: {required_permission}"},
        )

    ticket = await create_ws_ticket(
        identity=current_user,
        channel=channel,
        job_id=job_id,
    )

    return {
        "success": True,
        "data": {
            "ticket": ticket,
            "expires_in": 60,
            "channel": channel,
            "job_id": job_id,
        },
        "message": "Đã cấp WebSocket ticket",
    }