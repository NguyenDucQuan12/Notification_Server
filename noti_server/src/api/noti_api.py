from __future__ import annotations
from fastapi import APIRouter, Depends, WebSocket



from controllers.noti_controller import NotiController
from utils.auth import get_current_identity_from_websocket, identity_has_permission
from utils.redis_keys import job_stream_key, tenant_stream_key, user_stream_key, global_stream_key


# Router cho các endpoint liên quan đến thông báo. Hiện tại chỉ có WebSocket, chưa có REST API nào.
router = APIRouter(
    prefix="/noti",
    tags=["Notifications"],
)




@router.websocket("/ws/events")
async def websocket_user_events(websocket: WebSocket) -> None:
    """
    WebSocket nhận event theo user.  
    Client kết nối:  
    ws://127.0.0.1:8001/quan/noti/ws/events?ticket=<WEBSOCKET_TICKET>&last_event_id=0-0
    Quyền cần có:
    notification:subscribe  
    Đây là endpoint chính để client subscribe event theo user_id.
    """

    # Xác thực WebSocket ticket và lấy identity.
    # Nếu có lỗi xác thực, đóng WebSocket với code 4401 (Unauthorized).
    try:
        identity = await get_current_identity_from_websocket(websocket)
    except ValueError as exc:
        await websocket.close(code=4401, reason=str(exc))
        return

    # Kiểm tra channel trong ticket phải là "user", nếu không thì đóng WebSocket với code 4403 (Forbidden).
    if identity.channel != "user":
        await websocket.close(code=4403, reason="Ticket không hợp lệ cho channel này")
        return

    # Kiểm tra quyền "notification:subscribe", nếu không có thì đóng WebSocket với code 4403 (Forbidden).
    if not identity_has_permission(identity, "notification:subscribe"):
        await websocket.close(code=4403, reason="Ticket không có quyền truy cập")
        return

    # Tạo stream_key cho user dựa trên tenant_id và user_id trong identity. Ví dụ: "stream:noti:tenant:123:user:456". 
    # Cách tạo stream_key phải nhất quán với cách publish event trong API service và worker service, để đảm bảo client subscribe đúng stream.
    stream_key = user_stream_key(identity.tenant_id, identity.user_id)

    # Chạy hàm stream_live_events để lắng nghe event mới liên tục và gửi về client qua WebSocket.
    await NotiController.run_stream_websocket(
        websocket=websocket,
        identity=identity,
        stream_key=stream_key,
        channel="user",
    )


@router.websocket("/ws/tenant-dashboard")
async def websocket_tenant_dashboard(websocket: WebSocket) -> None:
    """
    WebSocket dashboard theo tenant.

    Client kết nối:
    ws://127.0.0.1:8001/quan/noti/ws/tenant-dashboard?token=<JWT>&last_event_id=0-0

    Quyền cần có:
    notification:tenant_dashboard

    Admin/superuser dùng endpoint này để theo dõi toàn bộ job trong tenant.
    """

    try:
        identity = await get_current_identity_from_websocket(websocket)
    except ValueError as exc:
        await websocket.close(code=4401, reason=str(exc))
        return

    if identity.channel != "tenant_dashboard":
        await websocket.close(code=4403, reason="Ticket channel mismatch")
        return

    if not identity_has_permission(identity, "notification:tenant_dashboard"):
        await websocket.close(code=4403, reason="Missing permission")
        return

    stream_key = tenant_stream_key(identity.tenant_id)

    await NotiController.run_stream_websocket(
        websocket=websocket,
        identity=identity,
        stream_key=stream_key,
        channel="tenant_dashboard",
    )


@router.websocket("/ws/jobs/{job_id}")
async def websocket_job_events(
    websocket: WebSocket,
    job_id: str,
) -> None:
    """
    WebSocket nhận event riêng của một job.

    Client kết nối:
    ws://127.0.0.1:8001/quan/noti/ws/jobs/<JOB_ID>?token=<JWT>&last_event_id=0-0

    Quyền cần có:
    notification:job_watch

    Lưu ý:
    - Endpoint này chỉ xác thực bằng token và tenant_id trong token.
    - Nếu muốn kiểm tra job_id có thật sự thuộc user không,
      có thể để API service cấp một job_watch_token riêng ngắn hạn.
    """

    try:
        identity = await get_current_identity_from_websocket(websocket)
    except ValueError as exc:
        await websocket.close(code=4401, reason=str(exc))
        return

    if identity.channel != "job":
        await websocket.close(code=4403, reason="Ticket channel mismatch")
        return

    if identity.job_id != job_id:
        await websocket.close(code=4403, reason="Ticket job_id mismatch")
        return

    if not identity_has_permission(identity, "notification:job_watch"):
        await websocket.close(code=4403, reason="Missing permission")
        return

    stream_key = job_stream_key(identity.tenant_id, job_id)

    await NotiController.run_stream_websocket(
        websocket=websocket,
        identity=identity,
        stream_key=stream_key,
        channel="job",
    )

@router.websocket("/ws/global-events")
async def websocket_global_events(websocket: WebSocket) -> None:
    """
    WebSocket nhận event toàn cục.

    Client kết nối:
    ws://127.0.0.1:8001/quan/noti/ws/global-events?ticket=<WEBSOCKET_TICKET>&last_event_id=0-0
    Endpoint này dành cho admin/superuser theo dõi toàn bộ event của hệ thống, phục vụ mục đích monitoring/logging.
    """
    # Xác thực WebSocket ticket và lấy identity.
    # Nếu có lỗi xác thực, đóng WebSocket với code 4401 (Unauthorized).
    print("Đã vào đây")
    try:
        identity = await get_current_identity_from_websocket(websocket)
    except ValueError as exc:
        await websocket.close(code=4401, reason=str(exc))
        return

    print("Đã vào đây 1")
    # Kiểm tra channel trong ticket phải là "global", nếu không thì đóng WebSocket với code 4403 (Forbidden).
    if identity.channel != "global":
        await websocket.close(code=4403, reason="Ticket không hợp lệ cho channel này")
        return

    print("Đã vào đây 2")
    # Kiểm tra quyền "notification:global_subscribe", nếu không có thì đóng WebSocket với code 4403 (Forbidden).
    if not identity_has_permission(identity, "notification:global_subscribe"):
        await websocket.close(code=4403, reason="Ticket không có quyền truy cập")
        return

    stream_key = global_stream_key()

    await NotiController.run_stream_websocket(
        websocket=websocket,
        identity=identity,
        stream_key=stream_key,
        channel="global",
    )
