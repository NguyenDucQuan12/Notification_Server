"""
Controller xử lý vòng đời của 1 kết nối WebSocket nhận thông báo.

Router (api/noti_api.py) chỉ lo xác thực, sau đó gọi
NotiController.run_stream_websocket(). Hàm này chịu trách nhiệm:

    ┌──────────────────── run_stream_websocket ─────────────────────┐
    │ 1. Đăng ký vào ConnectionManager (kiểm tra giới hạn kết nối)  │
    │ 2. Chạy SONG SONG 2 task:                                     │
    │    ├─ sender  : subscribe Hub -> connected -> replay -> live  │
    │    └─ receiver: đọc message từ client (ping, đóng kết nối)    │
    │ 3. Task nào kết thúc trước thì hủy task còn lại               │
    │ 4. Đóng kết nối với close code phù hợp, ghi log               │
    │ 5. Hủy đăng ký khỏi Hub và ConnectionManager                  │
    └───────────────────────────────────────────────────────────────┘

PHẦN LIVE DÙNG STREAM HUB (services/stream_hub.py):
Kết nối chỉ ĐĂNG KÝ vào Hub và đọc từ hàng đợi asyncio trong RAM; Hub dùng
vài kết nối Redis cho cả process. Nhờ vậy 1 process phục vụ được hàng nghìn kết nối.

VÌ SAO CẦN TASK RECEIVER?
Nếu chỉ GỬI mà không ĐỌC, server không biết client đã đóng tab cho tới lần gửi
kế tiếp (có thể tới 15s heartbeat), không trả lời được "ping", và lúc tắt server
phải chờ lâu. Có receiver: client vừa ngắt là hủy ngay sender và dọn tài nguyên.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect
from redis.exceptions import RedisError, ResponseError
from starlette.websockets import WebSocketState

from config.configs import settings
from services.connection_manager import ConnectionInfo, ConnectionRejected, connection_manager
from services.redis_client import get_redis_fast
from services.stream_hub import Subscription, stream_hub
from utils import close_codes
from utils.auth import Identity
from utils.events import fields_to_event
from utils.utils import (
    STREAM_START_ID,
    build_connected_message,
    build_heartbeat_message,
    build_pong_message,
    build_replay_incomplete_message,
    build_server_error_message,
    compare_stream_ids,
    normalize_last_event_id,
    safe_json_dumps,
)

logger = logging.getLogger("noti.ws")

# Chống client spam message: tối đa N message trong mỗi cửa sổ thời gian.
_CLIENT_MSG_LIMIT = 30
_CLIENT_MSG_WINDOW_SECONDS = 10.0


class SendTimeout(Exception):
    """Gửi 1 message tới client quá WS_SEND_TIMEOUT_SECONDS (client nhận quá chậm)."""

class SlowConsumer(Exception):
    """Hàng đợi của client trong Hub bị đầy: client nhận chậm hơn tốc độ event đến."""

class ClientMessageFlood(Exception):
    """Client gửi quá nhiều message trong thời gian ngắn."""

class _ClientSession:
    """
    Bọc WebSocket của 1 client, đảm bảo:
    - `_send_lock`: Chỉ 1 coroutine được gửi tại 1 thời điểm (sender và receiver đều có thể gửi:
      sender gửi event, receiver gửi pong). Gửi đồng thời có thể làm lẫn frame.
    - Mỗi lần gửi có timeout, tránh 1 client mạng chậm làm đầy bộ nhớ server.
    """

    def __init__(self, websocket: WebSocket, info: ConnectionInfo) -> None:
        self.websocket = websocket
        self.info = info
        self._send_lock = asyncio.Lock()

    async def send_json(self, data: dict[str, Any], *, is_event: bool = False) -> None:
        """
        Gửi dict đã JSON-hóa sẵn (Hub chỉ JSON-hóa 1 lần cho mọi subscriber).  
        is_event: True nếu là event, False nếu là message hệ thống (connected, pong, heartbeat...).
        """
        await self.send_text(safe_json_dumps(data), is_event=is_event)

    async def send_text(self, text: str, *, is_event: bool = False) -> None:
        """
        Gửi chuỗi JSON đã sẵn (Hub JSON-hóa 1 lần cho mọi subscriber).  
        is_event: True nếu là event, False nếu là message hệ thống (connected, pong, heartbeat...).
        """
        # Lấy lock để tránh 2 coroutine gửi cùng lúc (sender và receiver đều có thể gửi).
        async with self._send_lock:
            try:
                # Gửi với timeout để tránh 1 client mạng chậm làm đầy bộ nhớ server.
                await asyncio.wait_for(self.websocket.send_text(text), timeout=settings.WS_SEND_TIMEOUT_SECONDS)
            except asyncio.TimeoutError as exc:
                raise SendTimeout() from exc

        # Nếu là event, tăng counter để ghi log tổng kết phiên.
        if is_event:
            self.info.events_sent += 1

class NotiController:
    """Nghiệp vụ stream thông báo từ Redis Stream tới WebSocket client."""

    @staticmethod
    async def safe_close(websocket: WebSocket, code: int, reason: str = "") -> None:
        """
        Đóng WebSocket mà không bao giờ ném lỗi.

        Kết nối có thể đã đóng từ phía client hoặc mạng đã đứt; khi đó gọi
        close() sẽ lỗi RuntimeError — với ta không còn gì để làm nên bỏ qua.
        Có timeout để không bị treo khi mạng của client đã chết.  
        `reason` là lý do đóng kết nối, được cắt còn tối đa 123 ký tự (theo RFC 6455 - giới hạn chuẩn của WebSocket) và escape các ký tự không hợp lệ.
        """
        if websocket.application_state == WebSocketState.DISCONNECTED:
            return
        try:
            await asyncio.wait_for(
                websocket.close(code=code, reason=close_codes.safe_reason(reason)),
                timeout=2,
            )
        except Exception:  # pylint: disable=broad-except
            pass

    @staticmethod
    async def _detect_trimmed_history(stream_key: str, last_event_id: str) -> bool:
        """
        Kiểm tra có event nào client CHƯA nhận nhưng đã bị Redis xóa (do MAXLEN) không.

        Redis >= 7.0 có trường "max-deleted-entry-id" trong XINFO STREAM: ID lớn nhất
        từng bị xóa khỏi stream. Nếu ID đó > last_event_id của client nghĩa là có
        event sau mốc client đã nhận bị xóa mất => client cần đồng bộ lại qua REST API.

        Redis cũ hơn không có trường này -> trả False (không phát hiện được, không sao).
        """
        # Lấy thông tin stream, nếu stream chưa tồn tại (chưa có event nào) thì không mất gì.
        try:
            info = await get_redis_fast().xinfo_stream(stream_key)
        except ResponseError:
            return False

        # Lấy max-deleted-entry-id để so sánh với last_event_id của client.
        max_deleted = info.get("max-deleted-entry-id")
        if not max_deleted or max_deleted == STREAM_START_ID:
            return False

        # So sánh 2 stream ID để biết client có bỏ lỡ event nào đã bị xóa không
        return compare_stream_ids(str(max_deleted), last_event_id) > 0

    @staticmethod
    async def replay_stream(
        session: _ClientSession,
        *,
        stream_key: str,
        last_event_id: str,
        channel: str,
    ) -> str:
        """
        Gửi lại các event có ID > last_event_id, tối đa REPLAY_MAX_EVENTS event.

        Ví dụ: client đã nhận 100-0 rồi mất mạng; trong lúc đó có 101-0, 102-0.
        Client kết nối lại với last_event_id=100-0 -> server gửi 101-0, 102-0.

        Cách làm: XREVRANGE (đọc NGƯỢC từ mới nhất về cũ) lấy tối đa N+1 event.
        - Lấy được <= N event : đủ hết, đảo ngược lại cho đúng thứ tự cũ -> mới rồi gửi.
        - Lấy được  N+1 event : client bỏ lỡ quá nhiều, chỉ gửi N event MỚI NHẤT và
                                báo "replay_incomplete" để client đồng bộ qua REST API.
        So với bản cũ (XRANGE lặp tới hết stream): không còn trường hợp 1 client
        bị dội cả 10.000 event, và tốn đúng 1 lệnh Redis.

        Trả về cursor (ID event cuối cùng đã gửi) để phần live đọc tiếp từ đó,
        đảm bảo không trùng và không sót event giữa replay và live.
        """
        # Lấy số lượng event tối đa client được phép nhận trong 1 lần replay.
        limit = settings.REPLAY_MAX_EVENTS
        redis_fast = get_redis_fast()
        client_has_cursor = last_event_id != STREAM_START_ID

        # REPLAY_MAX_EVENTS=0: tắt replay, chỉ nhận event mới từ thời điểm kết nối.
        if limit == 0:
            latest = await redis_fast.xrevrange(stream_key, max="+", min="-", count=1)
            return latest[0][0] if latest else last_event_id

        # "(" = khoảng mở: lấy event có ID LỚN HƠN last_event_id, không lấy lại chính nó.
        rows = await redis_fast.xrevrange(stream_key, max="+", min=f"({last_event_id}", count=limit + 1)

        # Lấy đủ N event mới nhất, đảo ngược lại cho đúng thứ tự cũ -> mới rồi gửi.
        truncated = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()

        # Chỉ báo thiếu dữ liệu với client ĐÃ từng kết nối (có last_event_id).
        # Client mới hoàn toàn thì lấy dữ liệu ban đầu qua REST API.
        if client_has_cursor:
            reason = None
            if truncated:
                reason = "limit"
            elif await NotiController._detect_trimmed_history(stream_key, last_event_id):
                reason = "trimmed"

            if reason:
                logger.info(
                    "Replay incomplete",
                    extra={"conn_id": session.info.connection_id, "reason": reason},
                )
                await session.send_json(
                    build_replay_incomplete_message(
                        channel=channel,
                        reason=reason,
                        requested_last_event_id=last_event_id,
                        resumed_from_event_id=rows[0][0] if rows else None,
                    )
                )

        # Gửi các event cho client, cập nhật cursor để phần live đọc tiếp từ đó.
        cursor = last_event_id
        for stream_id, fields in rows:
            await session.send_json(fields_to_event(stream_id, fields), is_event=True)
            cursor = stream_id

        return cursor

    # ------------------------------------------------------------------
    # Live: nhận event mới từ Stream Hub
    # ------------------------------------------------------------------
    @staticmethod
    async def stream_live_events(
        session: _ClientSession,
        *,
        subscription: Subscription,
        cursor: str,
        channel: str,
    ) -> None:
        """
        Vòng lặp chính: lấy event từ hàng đợi của Hub và gửi cho client.

        - Có event -> bỏ qua nếu ID <= cursor (đã gửi lúc replay), còn lại thì gửi.
        - Hết HEARTBEAT_INTERVAL_SECONDS không có event -> gửi heartbeat giữ kết nối.
        - Hàng đợi bị đầy (client quá chậm) -> ném SlowConsumer, đóng 4408.
        """
        # Lấy hàng đợi asyncio của subscription, đọc event từ đó.
        queue = subscription.queue
        while True:
            # Nếu hàng đợi của subscription bị đầy, client nhận chậm hơn tốc độ event đến.
            if subscription.overflowed:
                raise SlowConsumer()

            try:
                # Lấy event từ hàng đợi, có timeout để gửi heartbeat nếu không có event nào.
                stream_id, text = await asyncio.wait_for(queue.get(), timeout=settings.HEARTBEAT_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                await session.send_json(build_heartbeat_message(channel, stream_healthy=stream_hub.healthy))
                continue

            # event này đã được gửi trong bước replay thì bỏ qua
            if compare_stream_ids(stream_id, cursor) <= 0:
                continue

            await session.send_text(text, is_event=True)
            cursor = stream_id

    @staticmethod
    async def _sender(
        session: _ClientSession,
        *,
        identity: Identity,
        stream_key: str,
        channel: str,
        last_event_id: str,
    ) -> None:
        """
        Task gửi. Chỉ kết thúc khi có lỗi hoặc bị hủy.

        THỨ TỰ QUAN TRỌNG: subscribe Hub TRƯỚC rồi mới replay. Event đến trong lúc
        replay sẽ nằm sẵn trong hàng đợi, không bị lọt (xem services/stream_hub.py).
        """
        # Đăng ký nhận event từ Hub, trả về subscription có hàng đợi asyncio.
        subscription = await stream_hub.subscribe(stream_key)
        try:
            # Gửi message "connected" ngay sau khi kết nối thành công, trước khi replay.
            await session.send_json(
                build_connected_message(
                    channel=channel,
                    last_event_id=last_event_id,
                    identity=identity,
                    connection_id=session.info.connection_id,
                    heartbeat_interval_ms=int(settings.HEARTBEAT_INTERVAL_SECONDS * 1000),
                )
            )

            # Replay: gửi lại các event client đã bỏ lỡ, trả về cursor (ID event cuối cùng đã gửi).
            cursor = await NotiController.replay_stream(
                session,
                stream_key=stream_key,
                last_event_id=last_event_id,
                channel=channel,
            )

            # Live: nhận event mới từ Stream Hub và gửi cho client, cho tới khi có lỗi hoặc bị hủy.
            await NotiController.stream_live_events(
                session,
                subscription=subscription,
                cursor=cursor,
                channel=channel,
            )
        finally:
            # Luôn hủy đăng ký (kể cả khi bị cancel) để Hub không giữ hàng đợi "mồ côi".
            stream_hub.unsubscribe(subscription)

    @staticmethod
    async def _receiver(session: _ClientSession) -> None:
        """
        Task nhận: đọc message client gửi lên.

        - Client gửi "ping" (chuỗi) hoặc {"type": "ping"} -> trả {"type": "pong"}.
          Hữu ích cho client trình duyệt (JS không gửi được ping tầng giao thức).
        - Message khác: bỏ qua (kênh này chỉ để server đẩy dữ liệu xuống).
        - Client đóng kết nối -> hàm return -> controller hủy task sender.
        """
        # Biến đếm số lượng message client gửi trong 1 cửa sổ thời gian, để chống spam.
        window_start = time.monotonic()
        count = 0

        while True:
            message = await session.websocket.receive()

            # Nếu client đã ngắt kết nối thì task này kết thúc, controller hủy task sender.
            if message["type"] == "websocket.disconnect":
                return

            # Giới hạn tần suất message của client.
            now = time.monotonic()
            # Đã quá cửa sổ thời gian (mặc định 10 phút) thì reset biến đếm.
            if now - window_start > _CLIENT_MSG_WINDOW_SECONDS:
                window_start, count = now, 0
            count += 1
            if count > _CLIENT_MSG_LIMIT:
                # Trong 10 phút mà client gửi quá 30 message -> coi là spam, đóng kết nối.
                raise ClientMessageFlood()

            # Kiểm tra nội dung message client gửi lên: nếu là ping thì trả pong, còn lại bỏ qua.
            text = message.get("text")
            if text is None and message.get("bytes") is not None:
                text = message["bytes"].decode("utf-8", errors="ignore")
            if NotiController._is_ping(text):
                await session.send_json(build_pong_message())

    @staticmethod
    def _is_ping(text: str | None) -> bool:
        """Nhận cả 2 dạng: chuỗi "ping" hoặc JSON {"type": "ping"}."""
        text = (text or "").strip()
        if text.lower() == "ping":
            return True
        if text.startswith("{") and len(text) <= 256:
            try:
                data = json.loads(text)
            except ValueError:
                return False
            return isinstance(data, dict) and data.get("type") == "ping"
        return False

    # ------------------------------------------------------------------
    # Hàm chính
    # ------------------------------------------------------------------
    @staticmethod
    async def run_stream_websocket(*, websocket: WebSocket, identity: Identity, stream_key: str, channel: str) -> None:
        """
        Phục vụ 1 kết nối WebSocket ĐÃ ĐƯỢC accept và xác thực.

        Hàm chỉ return khi kết nối đã kết thúc; mọi lỗi đều được xử lý bên trong.
        """

        # Mã kết nối ngắn, gắn vào mọi dòng log để truy vết.
        connection_id = uuid.uuid4().hex[:12]
        log_ctx = {
            "conn_id": connection_id,
            "tenant_id": identity.tenant_id,
            "user_id": identity.user_id,
            "channel": channel,
        }

        info = ConnectionInfo(
            connection_id=connection_id,
            websocket=websocket,
            identity=identity,
            channel=channel,
            stream_key=stream_key,
        )

        # 1. Kiểm tra giới hạn kết nối.
        try:
            await connection_manager.register(info)
        except ConnectionRejected as exc:
            logger.warning("WebSocket rejected", extra={**log_ctx, "reason": exc.reason})
            await NotiController.safe_close(websocket, exc.code, exc.reason)
            return

        # Lấy thông tin last_event_id từ query param, chuẩn hóa về dạng "0-0" nếu không hợp lệ.
        last_event_id = normalize_last_event_id(websocket.query_params.get("last_event_id"))
        session = _ClientSession(websocket, info)
        logger.info("WebSocket connected", extra={**log_ctx, "last_event_id": last_event_id})

        # 1.2. Chạy song song 2 task: sender và receiver. Task nào kết thúc trước thì hủy task còn lại.
        sender = asyncio.create_task(
            NotiController._sender(
                session,
                identity=identity,
                stream_key=stream_key,
                channel=channel,
                last_event_id=last_event_id,
            ),
            name=f"ws-sender-{connection_id}",
        )
        receiver = asyncio.create_task(
            NotiController._receiver(session),
            name=f"ws-receiver-{connection_id}",
        )
        tasks = {sender, receiver}

        close_code: int | None = None   # None = client đã tự ngắt, không cần đóng.
        close_reason = ""

        try:
            # 2. Chờ task nào kết thúc trước.
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finished = done.pop()
            exc = finished.exception()

            # 3. Phân loại nguyên nhân kết thúc -> close code.
            if exc is None or isinstance(exc, WebSocketDisconnect):
                pass  # Client ngắt kết nối bình thường.

            elif isinstance(exc, (SendTimeout, SlowConsumer)):
                logger.warning("Client quá chậm trong quá trình nhận thông báo", extra=log_ctx)
                close_code, close_reason = close_codes.SEND_TIMEOUT, "send_timeout"

            elif isinstance(exc, ClientMessageFlood):
                logger.warning("Client gửi quá nhiều message, đóng kết nối", extra=log_ctx)
                close_code, close_reason = close_codes.POLICY_VIOLATION, "too_many_messages"

            elif isinstance(exc, RedisError):
                logger.error("Lỗi Redis trong quá trình streaming", extra={**log_ctx, "error": repr(exc)})
                await NotiController._try_send_error(
                    session, "Hệ thống thông báo tạm thời gián đoạn, vui lòng kết nối lại sau.",
                    code="redis_unavailable",
                )
                close_code, close_reason = close_codes.TRY_AGAIN_LATER, "redis_unavailable"

            elif NotiController._is_disconnected(websocket):
                # Gửi lỗi vì client vừa ngắt (RuntimeError "send after close"...).
                pass

            else:
                # Lỗi không lường trước: ghi đầy đủ traceback vào log, client chỉ nhận thông báo chung.
                logger.error("Lỗi máy chủ thông báo, vui lòng kết nối lại.", extra=log_ctx, exc_info=exc)
                await NotiController._try_send_error(
                    session, "Lỗi máy chủ thông báo, vui lòng kết nối lại.", code="internal_error",
                )
                close_code, close_reason = close_codes.INTERNAL_ERROR, "internal_error"

        finally:
            # 4. Luôn hủy các task còn chạy (kể cả khi chính hàm này bị hủy lúc tắt server).
            #    Hủy sender đang chờ XREAD BLOCK -> redis-py ngắt kết nối đó, pool tạo mới khi cần.
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            if close_code is not None:
                await NotiController.safe_close(websocket, close_code, close_reason)

            # 5. Hủy đăng ký + ghi log tổng kết phiên.
            await connection_manager.unregister(connection_id)
            logger.info(
                "Kết thúc máy chủ thông báo",
                extra={
                    **log_ctx,
                    "close_code": close_code,
                    "events_sent": info.events_sent,
                    "duration_s": round(time.monotonic() - info.connected_at, 1),
                },
            )

    # ------------------------------------------------------------------
    @staticmethod
    def _is_disconnected(websocket: WebSocket) -> bool:
        """
        Kiểm tra xem kết nối WebSocket đã bị ngắt chưa (client đóng tab, mạng đứt...).
        """
        return (
            websocket.client_state == WebSocketState.DISCONNECTED
            or websocket.application_state == WebSocketState.DISCONNECTED
        )

    @staticmethod
    async def _try_send_error(session: _ClientSession, message: str, *, code: str) -> None:
        """
        Cố gửi thông báo lỗi cho client; thất bại cũng không sao (sắp đóng kết nối).
        """
        try:
            await session.send_json(build_server_error_message(message, code=code))
        except Exception:  # pylint: disable=broad-except
            pass
