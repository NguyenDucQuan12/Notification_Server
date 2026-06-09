from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets
from websockets.exceptions import ConnectionClosed


# ============================================================
# CẤU HÌNH
# ============================================================

# API server: nơi có login API và API xin WebSocket ticket
API_BASE_URL = "http://127.0.0.1:8000"

# Notification server: nơi có WebSocket /noti/ws/...
NOTI_WS_BASE_URL = "ws://127.0.0.1:1234/noti"

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

# Tài khoản test
TENANT_ID = "system"
USERNAME = "quan"
PASSWORD = "123456789"

# Channel muốn nghe:
# - "user"
# - "tenant_dashboard"
# - "job"
# - "global"
CHANNEL = "global"

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
        "tenant_id": TENANT_ID,
        "username": USERNAME,
        "password": PASSWORD,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            login_url,
            json=payload,
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

                    event_type = event.get("type")

                    # In event ra màn hình
                    print("=" * 80)
                    print("Nhận event:")
                    print(json.dumps(event, ensure_ascii=False, indent=2))

                    # Nếu server gửi event_id thì lưu lại.
                    # heartbeat/connected có thể không có event_id.
                    event_id = event.get("event_id")

                    if event_id:
                        save_last_event_id(str(event_id))
                        print(f"Đã lưu last_event_id={event_id}")

                    # Bạn có thể xử lý riêng từng loại event ở đây.
                    if event_type == "job.progress":
                        print(
                            f"Job {event.get('job_id')} progress={event.get('progress')}%"
                        )

                    elif event_type == "job.success":
                        print(
                            f"Job {event.get('job_id')} đã hoàn tất. "
                            f"result={event.get('result_object_key')}"
                        )

                    elif event_type == "job.failed":
                        print(
                            f"Job {event.get('job_id')} thất bại. "
                            f"error={event.get('error')}"
                        )

                    elif event_type == "role.created":
                        print(
                            f"Role mới được tạo: "
                            f"{event.get('role_id')} - {event.get('role_name')}"
                        )

                    elif event_type == "heartbeat":
                        print("Heartbeat từ notification server.")

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

            await asyncio.sleep(3)

        except ConnectionClosed as exc:
            print(f"WebSocket bị đóng: code={exc.code}, reason={exc.reason}")
            print("Sẽ reconnect sau 3 giây...")
            await asyncio.sleep(3)

        except OSError as exc:
            print(f"Lỗi mạng/WebSocket: {type(exc).__name__}: {exc}")
            print("Sẽ reconnect sau 3 giây...")
            await asyncio.sleep(3)

        except Exception as exc:
            print(f"Lỗi không xác định: {type(exc).__name__}: {exc}")
            print("Sẽ reconnect sau 3 giây...")
            await asyncio.sleep(3)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    asyncio.run(listen_notification_forever())