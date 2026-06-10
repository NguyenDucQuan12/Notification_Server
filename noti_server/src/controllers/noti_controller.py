from __future__ import annotations
from typing import Any
from fastapi import WebSocket, WebSocketDisconnect, status
from redis.exceptions import RedisError

from utils.auth import Identity
from config.configs import settings
from services.redis_client import get_redis_stream
from utils.events import fields_to_event
from utils.utils import safe_json_dumps, normalize_last_event_id, build_connected_message, build_heartbeat_message, build_server_error_message



# Tạo kết nối tới Redis
redis_client = get_redis_stream()
class NotiController:
    """
    Controller xử lý nghiệp vụ thông báo.

    Router chỉ nhận request và gọi controller.
    Controller chịu trách nhiệm:
    - validate dữ liệu nghiệp vụ
    - gọi query DB
    - xử lý lỗi
    """

    @staticmethod
    async def websocket_send_json( websocket: WebSocket, data: dict[str, Any], ) -> None:
        """
        Gửi JSON qua WebSocket đến client.

        FastAPI có websocket.send_json(), nhưng tự dumps giúp chúng ta kiểm soát ensure_ascii=False. Không bị lỗi font khi gửi tiếng Việt.  
        Ngoài ra còn giúp chúng ta có thể log dữ liệu trước khi gửi nếu cần, hoặc thêm metadata chung cho tất cả message sau này.
        """

        await websocket.send_text(safe_json_dumps(data))

    @staticmethod
    async def replay_stream( websocket: WebSocket, *, stream_key: str, last_event_id: str, current_state: dict[str, str], ) -> str:
        """
        Replay các event client đã bỏ lỡ.  

        Ví dụ:
        - Client đã nhận event 100-0.
        - Client mất mạng.
        - Server ghi tiếp 101-0, 102-0 vào Redis stream.
        - Client reconnect với last_event_id=100-0.
        - Server đọc event > 100-0 và gửi lại.

        Redis XRANGE với min='(100-0' nghĩa là lấy event lớn hơn 100-0, không lấy lại chính event 100-0.
        """

        # Chuẩn hóa last_event_id, nếu không hợp lệ thì mặc định là "0-0" (bắt đầu từ đầu stream).
        cursor = normalize_last_event_id(last_event_id)

        while True:
            # Lấy batch event mới từ Redis stream, bắt đầu từ cursor. Ví dụ: XRANGE stream_key (100-0 + COUNT 10.
            entries = await redis_client.xrange(
                stream_key,
                min=f"({cursor}",
                max="+",
                count=settings.REPLAY_BATCH_SIZE,
            )

            if not entries:
                break
            
            # Với mỗi event trong Redis Stream sẽ có stream_id và fields (dữ liệu event). Ví dụ: 101-0, {"type": "job_update", "job_id": "abc", ...}.
            for stream_id, fields in entries:
                # Cập nhật cursor sau khi đọc mỗi event, để lần sau đọc tiếp từ event tiếp theo. Ví dụ: cursor = 101-0.
                cursor = stream_id
                current_state["current_event_id"] = stream_id

                # Chuyển đổi stream_id và fields thành event chuẩn để gửi về client. Ví dụ: {"id": "101-0", "type": "job_update", "job_id": "abc", ...}.
                event = fields_to_event(stream_id, fields)
                # Gửi event về client qua WebSocket.
                await NotiController.websocket_send_json(websocket, event)

        return cursor

    @staticmethod
    async def stream_live_events(
        websocket: WebSocket,
        *,
        stream_key: str,
        current_event_id: str,
        channel: str,
        current_state: dict[str, str],
    ) -> None:
        """
        Nghe event mới liên tục bằng Redis XREAD BLOCK.  
        Đây là hàm chính cho việc stream live event về client sau khi đã replay xong. 

        Nếu chưa có event mới:
        - XREAD sẽ block trong STREAM_BLOCK_MS milliseconds.
        - Nếu hết thời gian vẫn không có event, server gửi heartbeat.
        """
        # Chuẩn hóa current_event_id trước khi dùng làm cursor. Nếu không hợp lệ thì mặc định là "0-0".
        cursor = normalize_last_event_id(current_event_id)

        while True:
            # Đọc các event mới từ Redis stream bắt đầu từ cursor, block nếu chưa có event mới.
            stream_response = await redis_client.xread(
                {stream_key: cursor},                      # Ví dụ: {"stream:noti:tenant:123:user:456": "101-0"} là stream_key và cursor hiện tại để bắt đầu đọc.
                block=settings.STREAM_BLOCK_MS,            # Block trong bao nhiêu ms nếu chưa có event mới. Ví dụ: 5000 (5 giây).
                count=10,                                  # Số lượng event tối đa để đọc mỗi lần. Ví dụ: 10 event.
            )

            # Nêú không có event mới sau khi block, gửi heartbeat về client để giữ kết nối và cho client biết server vẫn ổn.
            if not stream_response:
                await NotiController.websocket_send_json(
                    websocket,
                    build_heartbeat_message(channel),
                )
                continue
            
            # Với mỗi stream có thể có nhiều event mới, nên cần lặp qua tất cả event mới để gửi về client. Ví dụ: stream_response = [("stream:noti:tenant:123:user:456", [(101-0, {...}), (102-0, {...})])].
            for _, entries in stream_response:
                # Duyệt qua từng event mới trong stream_response. Ví dụ: entries = [(101-0, {...}), (102-0, {...})].
                for stream_id, fields in entries:
                    cursor = stream_id                                            # Cập nhật cursor sau khi đọc mỗi event mới, để lần sau đọc tiếp từ event tiếp theo. Ví dụ: cursor = 101-0.
                    current_state["current_event_id"] = stream_id                 # Cập nhật current_state với event_id mới nhất, để có thể dùng cho mục đích khác nếu cần.

                    event = fields_to_event(stream_id, fields)                    # Chuyển đổi stream_id và fields thành event chuẩn để gửi về client. Ví dụ: {"id": "101-0", "type": "job_update", "job_id": "abc", ...}.

                    await NotiController.websocket_send_json(websocket, event)    # Gửi event mới về client qua WebSocket.

    @staticmethod
    async def run_stream_websocket(
        *,
        websocket: WebSocket,
        identity: Identity,
        stream_key: str,
        channel: str,
    ) -> None:
        """
        Hàm dùng chung cho mọi WebSocket endpoint.

        Các bước:
        1. Lấy last_event_id từ query.
        2. Accept WebSocket.
        3. Gửi connected message.
        4. Replay event cũ.
        5. Nghe live event mới.
        6. Bắt lỗi disconnect/Redis/server.
        """

        # Lấy last_event_id từ query parameter, nếu không có thì mặc định là "0-0". Chuẩn hóa last_event_id trước khi dùng. Ví dụ: last_event_id = "100-0".
        last_event_id = normalize_last_event_id(
            websocket.query_params.get("last_event_id", "0-0")
        )

        # Lưu trạng thái hiện tại của stream, ví dụ current_event_id, vào current_state để có thể cập nhật và sử dụng xuyên suốt trong quá trình replay và stream live. Ví dụ: current_state = {"current_event_id": "100-0"}.
        current_state = {
            "current_event_id": last_event_id,
        }

        # Chấp nhận kết nối WebSocket sau khi đã xác thực và kiểm tra quyền. Nếu không accept thì client sẽ không thể nhận được message nào từ server, kể cả message lỗi.
        await websocket.accept()

        try:
            # Gửi message xác nhận đã kết nối thành công, kèm thông tin channel, last_event_id và identity. Client có thể dùng thông tin này để hiển thị trạng thái kết nối hoặc debug nếu cần.
            await NotiController.websocket_send_json(
                websocket,
                build_connected_message(
                    channel=channel,
                    last_event_id=last_event_id,
                    identity=identity,
                ),
            )

            # Replay các event cũ mà client đã bỏ lỡ dựa trên last_event_id. Hàm này sẽ trả về current_event_id sau khi replay xong, để chúng ta có thể tiếp tục stream live từ event tiếp theo.
            current_event_id = await NotiController.replay_stream(
                websocket,
                stream_key=stream_key,
                last_event_id=last_event_id,
                current_state=current_state,
            )

            # Sau khi replay xong, tiếp tục stream live các event mới liên tục. Hàm này sẽ block và chỉ return khi có lỗi hoặc client disconnect.
            await NotiController.stream_live_events(
                websocket,
                stream_key=stream_key,
                current_event_id=current_event_id,
                channel=channel,
                current_state=current_state,
            )

        # Bắt lỗi WebSocketDisconnect khi client chủ động ngắt kết nối, không cần gửi message lỗi hay làm gì thêm, chỉ cần return để dừng hàm.
        except WebSocketDisconnect:
            return

        # Bắt lỗi RedisError khi có lỗi liên quan đến Redis, gửi message lỗi về client và đóng WebSocket với code 1011 (Internal Error).
        except RedisError as e:
            try:
                # Gửi lỗi tới client
                await NotiController.websocket_send_json(
                    websocket,
                    build_server_error_message(f"Lỗi từ máy chủ Redis: {str(e)}"),
                )
                # Đóng kết nối vì đã xay ra lỗi thì ko cố gửi
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            except Exception:
                pass

        except Exception as e:
            try:
                await NotiController.websocket_send_json(
                    websocket,
                    build_server_error_message(f"Lỗi từ Notification Server: {str(e)}"),
                )
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            except Exception:
                pass