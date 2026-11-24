from __future__ import annotations

import argparse
import asyncio
import random
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets
from websockets.exceptions import ConnectionClosed


# ============================================================
# CẤU HÌNH
# ============================================================

# Mọi giá trị bên dưới có thể đổi bằng tham số dòng lệnh, xem: python listen_noti_server.py -h

# API server: nơi có login API và API xin WebSocket ticket
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

# Notification server: nơi có WebSocket /noti/ws/...
NOTI_WS_BASE_URL = os.getenv("NOTI_WS_BASE_URL", "ws://127.0.0.1:1234/noti")

# Đường dẫn login API.
LOGIN_PATH = "/auth/login"

# Đường dẫn xin WebSocket ticket.
TICKET_PATH = "/notifications/ws-ticket"

# Nếu API login của bạn nhận JSON:
# {
#   "username": "...",
#   "password": "..."
# }
LOGIN_AS_FORM = True

# Tài khoản test (đổi bằng --username/--password hoặc biến môi trường NOTI_USERNAME/NOTI_PASSWORD)
TENANT_ID = "system"
USERNAME = os.getenv("NOTI_USERNAME", "quan")
PASSWORD = os.getenv("NOTI_PASSWORD", "123456789")

# Channel muốn nghe (đổi bằng --channel):
# - "user"             : thông báo gửi riêng cho tài khoản này  (quyền notification:subscribe)
# - "tenant_dashboard" : mọi thông báo của tenant              (quyền notification:tenant_dashboard)
# - "job"              : tiến trình của 1 job, cần --job        (quyền notification:job_watch)
# - "global"           : mọi thông báo của hệ thống             (quyền notification:global_subscribe)
CHANNEL = "user"

# In toàn bộ JSON của event (--verbose)
VERBOSE = False

# Nếu CHANNEL = "job" thì cần job_id
JOB_ID: str | None = None

# File lưu last_event_id để reconnect không bị mất event
LAST_EVENT_FILE = Path(f".last_event_id_{CHANNEL}.txt")


# ============================================================
# HELPER
# ============================================================

def read_last_event_id() -> str:
    """
    Đọc last_event_id từ file.

    Nếu chưa có file thì trả 0-0, nghĩa là đọc từ đầu stream.
    """

    if not LAST_EVENT_FILE.exists():
        return "0-0"

    value = LAST_EVENT_FILE.read_text(encoding="utf-8").strip()

    return value or "0-0"


def save_last_event_id(event_id: str) -> None:
    """
    Lưu event_id mới nhất vào file.

    Khi WebSocket bị mất kết nối, lần sau client gửi lại last_event_id này
    để notification server replay các event bị lỡ.
    """

    LAST_EVENT_FILE.write_text(event_id, encoding="utf-8")


def get_ws_path(channel: str, job_id: str | None = None) -> str:
    """
    Trả về WebSocket path tương ứng với channel.

    Phải khớp với router notification server:
    - /ws/events
    - /ws/tenant-dashboard
    - /ws/jobs/{job_id}
    - /ws/global-events
    """

    if channel == "user":
        return "/ws/events"

    if channel == "tenant_dashboard":
        return "/ws/tenant-dashboard"

    if channel == "job":
        if not job_id:
            raise ValueError("CHANNEL='job' thì bắt buộc phải có JOB_ID")

        return f"/ws/jobs/{job_id}"

    if channel == "global":
        return "/ws/global-events"

    raise ValueError(f"Channel không hợp lệ: {channel}")


def extract_access_token(response_json: dict[str, Any]) -> str:
    """
    Lấy access_token từ response login.

    Hỗ trợ nhiều dạng response:
    1. {"access_token": "..."}
    2. {"token": {"access_token": "..."}}
    3. {"data": {"access_token": "..."}}
    4. {"data": {"token": {"access_token": "..."}}}
    """

    # Dạng 1:
    if "access_token" in response_json:
        return str(response_json["access_token"])

    # Dạng 2:
    token = response_json.get("token")

    if isinstance(token, dict):
        access_token = token.get("access_token")

        if access_token:
            return str(access_token)

    if isinstance(token, str):
        return token

    # Dạng 3, 4:
    data = response_json.get("data")

    if isinstance(data, dict):
        if "access_token" in data:
            return str(data["access_token"])

        token_data = data.get("token")

        if isinstance(token_data, dict):
            access_token = token_data.get("access_token")

            if access_token:
                return str(access_token)

        if isinstance(token_data, str):
            return token_data

    raise ValueError(f"Không tìm thấy access_token trong response: {response_json}")


def extract_ticket(response_json: dict[str, Any]) -> str:
    """
    Lấy ticket từ response xin WebSocket ticket.
    """

    if "ticket" in response_json:
        return str(response_json["ticket"])

    data = response_json.get("data")

    if isinstance(data, dict) and "ticket" in data:
        return str(data["ticket"])

    raise ValueError(f"Không tìm thấy ticket trong response: {response_json}")


# ============================================================
# HTTP API CLIENT
# (Mọi lần chờ kết nối lại đều cộng thêm thời gian ngẫu nhiên - "jitter" - để khi server khởi động lại,
#  hàng nghìn client không cùng ập vào API/Notification Server đúng một thời điểm.)
# ============================================================

async def login_and_get_token() -> str:
    """
    Gọi API login và lấy access_token.

    API /auth/login hiện yêu cầu:
    {
        "tenant_id": "...",
        "username": "...",
        "password": "..."
    }
    """

    login_url = f"{API_BASE_URL}{LOGIN_PATH}"

    payload = {
        # "tenant_id": TENANT_ID,
        "username": USERNAME,
        "password": PASSWORD,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            login_url,
            data=payload,       # Nếu dữ liệu nhận vào là json thì là data, nếu dữ liệu là json thì là json
        )

        # In rõ lỗi 422 để biết FastAPI đang thiếu field nào.
        if response.status_code != 200:
            print("Login failed")
            print("Status:", response.status_code)
            print("Response:", response.text)

        response.raise_for_status()

        response_json = response.json()

        access_token = extract_access_token(response_json)

        return access_token


async def get_ws_ticket(
    *,
    access_token: str,
    channel: str,
    job_id: str | None = None,
) -> str:
    """
    Gọi API server để xin WebSocket ticket.

    API này nên kiểm tra:
    - access_token hợp lệ
    - user có quyền với channel không
    - nếu channel=job thì job_id có thuộc user/tenant không

    Sau đó server trả:
    {
      "success": true,
      "data": {
        "ticket": "..."
      }
    }
    """

    ticket_url = f"{API_BASE_URL}{TICKET_PATH}"

    params: dict[str, str] = {
        "channel": channel,
    }

    if job_id:
        params["job_id"] = job_id

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            ticket_url,
            params=params,
            headers=headers,
        )

        response.raise_for_status()

        response_json = response.json()

        ticket = extract_ticket(response_json)

        return ticket


# ============================================================
# WEBSOCKET CLIENT
# ============================================================

async def listen_notification_forever() -> None:
    """
    Luồng chính:

    1. Login lấy token.
    2. Xin ticket.
    3. Mở WebSocket.
    4. Nhận event.
    5. Lưu last_event_id.
    6. Nếu mất kết nối thì xin ticket mới và reconnect.
    """

    print("Đang đăng nhập API server...")

    access_token = await login_and_get_token()

    print("Đăng nhập thành công, đã nhận access_token.")

    while True:
        try:
            last_event_id = read_last_event_id()

            print(f"Đang xin WebSocket ticket, channel={CHANNEL}, last_event_id={last_event_id}")

            ticket = await get_ws_ticket(
                access_token=access_token,
                channel=CHANNEL,
                job_id=JOB_ID,
            )

            ws_path = get_ws_path(CHANNEL, JOB_ID)

            query = urlencode(
                {
                    "ticket": ticket,
                    "last_event_id": last_event_id,
                }
            )

            ws_url = f"{NOTI_WS_BASE_URL}{ws_path}?{query}"

            print(f"Đang kết nối WebSocket: {ws_url}")

            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as websocket:
                print("Đã kết nối WebSocket.")

                async for raw_message in websocket:
                    try:
                        event = json.loads(raw_message)
                    except json.JSONDecodeError:
                        print("Nhận message không phải JSON:", raw_message)
                        continue

                    # Message hệ thống của Notification Server dùng "type" (connected, heartbeat, pong...).
                    # Thông báo từ API Server (qua outbox) dùng "event_type" / "notification_type".
                    event_type = (
                        event.get("type")
                        or event.get("event_type")
                        or event.get("notification_type")
                    )
                    now = datetime.now().strftime("%H:%M:%S")

                    if VERBOSE:
                        print("=" * 80)
                        print(json.dumps(event, ensure_ascii=False, indent=2))

                    if event_type == "heartbeat":
                        print(f"[{now}] heartbeat")
                        continue

                    if event_type == "connected":
                        print(f"[{now}] KẾT NỐI THÀNH CÔNG  {json.dumps(event, ensure_ascii=False)}")
                        continue

                    if event_type == "replay_incomplete":
                        # Server không gửi lại đủ các event đã lỡ -> nên gọi REST API để đồng bộ.
                        print(f"[{now}] CẢNH BÁO: có thể đã mất event (reason={event.get('reason')}), cần đồng bộ lại qua API.")
                        continue

                    # "stream_id" (= event_id) là Redis Stream ID, dùng làm last_event_id khi reconnect.
                    # ID nghiệp vụ gốc (nếu có) nằm ở "source_event_id".
                    event_id = event.get("stream_id") or event.get("event_id")

                    content = event.get("content") if isinstance(event.get("content"), dict) else {}
                    title = content.get("title") or event.get("title") or ""
                    body = content.get("body") or event.get("message") or ""

                    print("-" * 80)
                    print(f"[{now}] THÔNG BÁO  type={event_type}  stream_id={event_id}")
                    if title:
                        print(f"           {title}")
                    if body:
                        print(f"           {body}")

                    # Bạn có thể xử lý riêng từng loại event ở đây.
                    if event_type == "job.progress":
                        print(f"           Job {event.get('job_id')} progress={event.get('progress')}%")
                    elif event_type == "job.success":
                        print(f"           Job {event.get('job_id')} hoàn tất, result={event.get('result_object_key')}")
                    elif event_type == "job.failed":
                        print(f"           Job {event.get('job_id')} thất bại, error={event.get('error')}")

                    if event_id:
                        save_last_event_id(str(event_id))

        except KeyboardInterrupt:
            print("Dừng client.")
            return

        except httpx.HTTPStatusError as exc:
            print(
                "HTTP error:",
                exc.response.status_code,
                exc.response.text,
            )

            # Nếu token hết hạn hoặc sai, login lại.
            if exc.response.status_code in (401, 403):
                print("Token có thể hết hạn hoặc không đủ quyền, đăng nhập lại...")
                access_token = await login_and_get_token()

            await asyncio.sleep(3 + random.uniform(0, 1))

        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
            reason = exc.rcvd.reason if exc.rcvd else ""
            print(f"WebSocket bị đóng: code={code}, reason={reason}")

            # Xử lý theo bảng close code (noti_server/src/utils/close_codes.py).
            if code in (4400, 4403):
                print("Request sai hoặc không đủ quyền, dừng client (reconnect cũng vô ích).")
                return
            if code in (1013, 4429):
                print("Server quá tải / quá nhiều kết nối, reconnect sau 15 giây...")
                await asyncio.sleep(15 + random.uniform(0, 7))
            elif code == 1012:
                print("Server đang khởi động lại, reconnect sau 1 giây...")
                await asyncio.sleep(1 + random.uniform(0, 1))
            else:
                print("Sẽ reconnect sau 3 giây...")
                await asyncio.sleep(3 + random.uniform(0, 1))

        except OSError as exc:
            print(f"Lỗi mạng/WebSocket: {type(exc).__name__}: {exc}")
            print("Sẽ reconnect sau 3 giây...")
            await asyncio.sleep(3 + random.uniform(0, 1))

        except Exception as exc:
            print(f"Lỗi không xác định: {type(exc).__name__}: {exc}")
            print("Sẽ reconnect sau 3 giây...")
            await asyncio.sleep(3 + random.uniform(0, 1))


# ============================================================
# MAIN
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Client nghe thông báo realtime từ Notification Server.")
    parser.add_argument("--channel", default=CHANNEL,
                        choices=["user", "tenant_dashboard", "job", "global"],
                        help="Channel muốn nghe (mặc định: user).")
    parser.add_argument("--job", default=None, help="job_id, bắt buộc khi --channel job.")
    parser.add_argument("--username", default=USERNAME)
    parser.add_argument("--password", default=PASSWORD)
    parser.add_argument("--fresh", action="store_true",
                        help="Bỏ last_event_id đã lưu, chỉ nhận thông báo mới từ bây giờ.")
    parser.add_argument("-v", "--verbose", action="store_true", help="In toàn bộ JSON của event.")
    return parser.parse_args()


if __name__ == "__main__":
    _args = parse_args()

    CHANNEL = _args.channel
    JOB_ID = _args.job
    USERNAME = _args.username
    PASSWORD = _args.password
    VERBOSE = _args.verbose
    LAST_EVENT_FILE = Path(f".last_event_id_{CHANNEL}{'_' + JOB_ID if JOB_ID else ''}.txt")

    if _args.fresh:
        # Redis Stream ID có dạng "<mili giây>-<số thứ tự>". Lấy thời điểm hiện tại làm cursor
        # => server chỉ gửi event phát sinh SAU lúc này. (Xóa file = đọc lại từ đầu stream "0-0".)
        import time
        LAST_EVENT_FILE.write_text(f"{int(time.time() * 1000)}-0", encoding="utf-8")

    try:
        asyncio.run(listen_notification_forever())
    except KeyboardInterrupt:
        print("Dừng client.")
