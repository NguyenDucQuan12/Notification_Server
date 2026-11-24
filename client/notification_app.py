"""
Ứng dụng nhận thông báo realtime (customtkinter).

Luồng hoạt động:
    1. Người dùng đăng nhập  -> POST /auth/login (API Server)  -> JWT
    2. Tải hộp thư           -> GET  /notifications/feed       -> lịch sử + số chưa đọc (SQL Server)
    3. Xin ticket            -> POST /notifications/ws-ticket?channel=user
    4. Mở WebSocket          -> ws://<noti>/noti/ws/events?ticket=...&last_event_id=...
    5. Nhận thông báo realtime, hiện lên danh sách + popup góc màn hình
    6. Mất kết nối -> tự kết nối lại (backoff + jitter), đồng bộ lại hộp thư qua REST
    7. Công việc (job): nút "Tải file" -> POST /jobs/upload. Mỗi job chưa xong được theo dõi bằng
       1 WebSocket channel=job (/ws/jobs/{job_id}) để hiện thanh tiến trình realtime
       (job.queued -> job.started -> job.progress -> job.succeeded / job.failed), xong thì tự đóng.

Kiến trúc 3 lớp, tách biệt để giao diện không bao giờ bị "đơ":
    - ApiClient       : gọi REST bằng httpx (đồng bộ), chạy trong thread nền; tự đăng nhập lại khi token hết hạn.
    - RealtimeWorker  : 1 thread riêng chạy asyncio giữ WebSocket, đẩy sự kiện vào queue.
    - JobWatcher      : giống RealtimeWorker nhưng nghe stream của 1 job, tự dừng khi job kết thúc.
    - NotificationApp : giao diện Tk. CHỈ thread chính được chạm vào widget; sự kiện lấy từ queue
                        mỗi 100ms, mỗi lần tối đa N sự kiện (nhận dồn dập vẫn mượt).

Chạy:
    cd client
    python notification_app.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from tkinter import filedialog
from typing import Any, Callable
from urllib.parse import urlencode

import customtkinter as ctk
import httpx
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

# ============================================================
# CẤU HÌNH
# ============================================================

APP_TITLE = "Notification Center"
DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_WS_URL = "ws://127.0.0.1:1234/noti"

# Lưu URL server + username lần đăng nhập trước (KHÔNG lưu mật khẩu).
SETTINGS_FILE = Path.home() / ".notification_app.json"

FEED_PAGE_SIZE = 50          # Số thông báo tải từ REST khi mở app / đồng bộ lại
MAX_CARDS = 200              # Số thẻ thông báo tối đa trên màn hình (giới hạn để Tk luôn nhẹ)
UI_POLL_MS = 100             # Chu kỳ lấy sự kiện từ queue
UI_EVENTS_PER_TICK = 50      # Số sự kiện tối đa xử lý mỗi chu kỳ
TOAST_SECONDS = 4.5
MAX_TOASTS = 3

JOB_RECENT_LIMIT = 5         # Số job gần nhất tải về khi đăng nhập
MAX_JOB_CARDS = 4            # Số thẻ job hiển thị tối đa
MAX_JOB_WATCHERS = 10        # Số job theo dõi realtime cùng lúc (mỗi job 1 WebSocket)
UPLOAD_TIMEOUT_SECONDS = 300

# Trạng thái job -> (nhãn, màu). "retrying" chỉ dùng để hiển thị (DB vẫn là queued/processing).
JOB_STATUS_STYLE = {
    "queued": ("Đang chờ", "#9CA3AF"),
    "processing": ("Đang xử lý", "#3B82F6"),
    "retrying": ("Đang thử lại", "#F59E0B"),
    "success": ("Hoàn tất", "#22C55E"),
    "failed": ("Thất bại", "#EF4444"),
    "cancelled": ("Đã hủy", "#9CA3AF"),
}
JOB_TERMINAL = {"success", "failed", "cancelled"}
# notification_type của job -> trạng thái hiển thị
JOB_STAGE_STATUS = {
    "job.queued": "queued",
    "job.started": "processing",
    "job.progress": "processing",
    "job.retrying": "retrying",
    "job.succeeded": "success",
    "job.failed": "failed",
}

SEVERITY_COLORS = {
    "info": "#3B82F6",
    "success": "#22C55E",
    "warning": "#F59E0B",
    "error": "#EF4444",
    "critical": "#B91C1C",
}
STATUS_STYLE = {
    "connected": ("● Đã kết nối", "#22C55E"),
    "connecting": ("● Đang kết nối...", "#F59E0B"),
    "reconnecting": ("● Mất kết nối, đang thử lại...", "#F59E0B"),
    "error": ("● Lỗi kết nối", "#EF4444"),
    "stopped": ("● Đã ngắt", "#9CA3AF"),
}


# ============================================================
# TIỆN ÍCH
# ============================================================

def load_settings() -> dict[str, str]:
    try:
        return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - lần đầu chạy / tệp hỏng
        return {}


def save_settings(data: dict[str, str]) -> None:
    try:
        SETTINGS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


def decode_jwt_payload(token: str) -> dict[str, Any]:
    """Đọc payload của JWT để hiển thị tên người dùng (KHÔNG xác thực chữ ký - việc đó do server làm)."""
    try:
        part = token.split(".")[1]
        part += "=" * (-len(part) % 4)
        return json.loads(base64.urlsafe_b64decode(part))
    except Exception:  # noqa: BLE001
        return {}


def format_time(value: str | None) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.astimezone().strftime("%H:%M  %d/%m/%Y")
    except ValueError:
        return str(value)


@dataclass
class NotificationItem:
    notification_id: str
    title: str
    body: str
    notification_type: str = ""
    severity: str = "info"
    created_at: str = ""
    state: str = "unread"
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def is_unread(self) -> bool:
        return self.state == "unread"

    @classmethod
    def from_feed(cls, row: dict[str, Any]) -> "NotificationItem":
        """Từ 1 dòng của GET /notifications/feed (SQL Server)."""
        return cls(
            notification_id=str(row["notification_id"]),
            title=row.get("title") or "",
            body=row.get("body") or row.get("short_body") or "",
            notification_type=row.get("notification_type") or "",
            severity=row.get("severity") or "info",
            created_at=str(row.get("created_at") or ""),
            state=row.get("state") or "unread",
        )

    @classmethod
    def from_realtime(cls, event: dict[str, Any]) -> "NotificationItem | None":
        """Từ 1 event WebSocket (Redis Stream). Trả None nếu không phải thông báo."""
        notification_id = event.get("notification_id")
        if not notification_id:
            return None
        content = event.get("content") if isinstance(event.get("content"), dict) else {}
        return cls(
            notification_id=str(notification_id),
            title=content.get("title") or event.get("title") or "(không có tiêu đề)",
            body=content.get("body") or event.get("message") or "",
            notification_type=event.get("notification_type") or event.get("event_type") or "",
            severity=event.get("severity") or "info",
            created_at=str(event.get("created_at") or datetime.now().astimezone().isoformat()),
            state="unread",
        )


@dataclass
class JobState:
    """Trạng thái 1 job hiển thị trên giao diện (gộp từ REST /jobs và các thông báo job.*)."""
    job_id: str
    filename: str = ""
    status: str = "queued"
    progress: int = 0
    message: str = ""
    error: str = ""
    created_at: str = ""

    @property
    def is_terminal(self) -> bool:
        return self.status in JOB_TERMINAL

    def apply_rest(self, row: dict[str, Any]) -> None:
        """Cập nhật từ GET /jobs hoặc /jobs/{id}. REST là nguồn sự thật nên ghi đè hết."""
        self.filename = row.get("filename") or self.filename
        self.status = row.get("status") or self.status
        self.progress = int(row.get("progress") or 0)
        self.message = row.get("message") or ""
        self.error = row.get("error") or ""
        self.created_at = str(row.get("created_at") or self.created_at)

    def apply_event(self, event: dict[str, Any]) -> bool:
        """
        Cập nhật từ 1 thông báo job.* (realtime). Trả False nếu event bị bỏ qua.

        Event đến từ 2 stream (user + job) nên có thể lệch thứ tự: trạng thái kết thúc
        (success/failed) không bị event cũ hơn ghi đè, và job.queued không kéo lùi job đang chạy.
        """
        status = JOB_STAGE_STATUS.get(str(event.get("notification_type") or ""))
        if status is None:
            return False
        if self.is_terminal and status not in JOB_TERMINAL:
            return False
        if status == "queued" and self.status != "queued":
            return False

        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        content = event.get("content") if isinstance(event.get("content"), dict) else {}
        self.status = status
        self.filename = data.get("filename") or self.filename
        if status == "success":
            self.progress = 100
        elif data.get("progress") is not None and status != "queued":
            self.progress = int(data["progress"])
        self.message = content.get("body") or self.message
        self.error = data.get("error") or (self.error if status == "failed" else "")
        return True


# ============================================================
# REST API CLIENT
# ============================================================

class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class ApiClient:
    """
    Gọi API Server. Dùng trong thread nền (httpx đồng bộ).

    Token JWT hết hạn sau ACCESS_TOKEN_EXPIRE_MINUTES (mặc định 15 phút): khi gặp 401,
    client tự đăng nhập lại bằng thông tin đã nhập (chỉ giữ trong bộ nhớ) rồi gọi lại 1 lần.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=15.0)
        self._lock = threading.Lock()
        self._username = ""
        self._password = ""
        self.token = ""
        self.profile: dict[str, Any] = {}

    def close(self) -> None:
        self._http.close()

    # ---------------- Đăng nhập ----------------
    def login(self, username: str = "quan", password: str = "123456789") -> dict[str, Any]:
        try:
            resp = self._http.post(
                f"{self.base_url}/auth/login",
                data={"username": username, "password": password},  # API nhận FORM, không nhận JSON
            )
        except httpx.HTTPError as exc:
            raise ApiError(f"Không kết nối được API server ({self.base_url}): {type(exc).__name__}") from exc

        if resp.status_code in (400, 401, 403, 404):
            raise ApiError("Sai tên đăng nhập hoặc mật khẩu")
        if resp.status_code == 429:
            raise ApiError("Đăng nhập quá nhiều lần, hãy thử lại sau ít phút")
        if resp.status_code != 200:
            raise ApiError(f"Đăng nhập thất bại (HTTP {resp.status_code})")

        data = resp.json()
        token = (data.get("data") or {}).get("token") or {}
        access_token = token.get("access_token") if isinstance(token, dict) else token
        access_token = access_token or data.get("access_token")
        if not access_token:
            raise ApiError("Phản hồi đăng nhập không có access_token")

        with self._lock:
            self._username, self._password = username, password
            self.token = access_token
            self.profile = decode_jwt_payload(access_token)
        return self.profile

    def _relogin(self) -> None:
        self.login(self._username, self._password)

    # ---------------- Gọi API có xác thực ----------------
    @staticmethod
    def _error_message(resp: httpx.Response) -> str:
        """Lấy detail.message mà API trả về (FastAPI HTTPException), nếu có."""
        try:
            detail = resp.json().get("detail")
            if isinstance(detail, dict) and detail.get("message"):
                return str(detail["message"])
            if isinstance(detail, str):
                return detail
        except Exception:  # noqa: BLE001 - body không phải JSON
            pass
        return resp.text[:200]

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        for attempt in range(2):
            headers = {"Authorization": f"Bearer {self.token}"}
            try:
                resp = self._http.request(method, f"{self.base_url}{path}", headers=headers, **kwargs)
            except httpx.HTTPError as exc:
                raise ApiError(f"Không kết nối được API server: {type(exc).__name__}") from exc
            if resp.status_code == 401 and attempt == 0:
                self._relogin()  # Token hết hạn -> đăng nhập lại rồi thử lại 1 lần
                continue
            if resp.status_code >= 400:
                raise ApiError(
                    f"HTTP {resp.status_code}: {self._error_message(resp)}", status=resp.status_code,
                )
            return resp
        raise ApiError("Phiên đăng nhập hết hạn, hãy đăng nhập lại", status=401)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        return self._send(method, path, **kwargs).json()

    def get_ws_ticket(self, channel: str = "user", job_id: str | None = None) -> str:
        params = {"channel": channel}
        if job_id:
            params["job_id"] = job_id
        data = self._request("POST", "/notifications/ws-ticket", params=params)
        return str((data.get("data") or {}).get("ticket") or data.get("ticket"))

    # ---------------- Job ----------------
    def upload_job(self, file_path: str) -> dict[str, Any]:
        """Upload file -> tạo job. Đọc file vào bộ nhớ để gửi lại được khi phải đăng nhập lại."""
        path = Path(file_path)
        content = path.read_bytes()
        data = self._request(
            "POST", "/jobs/upload",
            files={"file": (path.name, content, "application/octet-stream")},
            timeout=UPLOAD_TIMEOUT_SECONDS,
        )
        return data["data"]["job"]

    def list_jobs(self, limit: int = JOB_RECENT_LIMIT) -> list[dict[str, Any]]:
        return list(self._request("GET", "/jobs", params={"limit": limit}).get("data") or [])

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}")["data"]

    def download_result(self, job_id: str, destination: str) -> str:
        resp = self._send("GET", f"/jobs/{job_id}/result", timeout=UPLOAD_TIMEOUT_SECONDS)
        Path(destination).write_bytes(resp.content)
        return destination

    def get_feed(self, limit: int = FEED_PAGE_SIZE) -> tuple[list[NotificationItem], int]:
        data = self._request("GET", "/notifications/feed", params={"limit": limit})["data"]
        return [NotificationItem.from_feed(r) for r in data["items"]], int(data["unread_count"])

    def mark_read(self, notification_id: str) -> None:
        self._request("POST", f"/notifications/{notification_id}/read")

    def mark_all_read(self) -> None:
        self._request("POST", "/notifications/read-all")


# ============================================================
# REALTIME WORKER (WebSocket trong thread riêng)
# ============================================================

class RealtimeWorker(threading.Thread):
    """
    Giữ kết nối WebSocket tới Notification Server và đẩy sự kiện vào `events` (queue.Queue):
        ("status", "connected" | "connecting" | "reconnecting" | "error" | "stopped", message)
        ("resync", None, None)      -> UI nên tải lại hộp thư qua REST (vừa kết nối / kết nối lại)
        ("notification", dict, None)

    Kết nối lại: backoff tăng dần 1s, 2s, 4s ... tối đa 30s, cộng JITTER ngẫu nhiên.
    Jitter rất quan trọng khi có hàng nghìn người dùng: nếu Notification Server khởi động lại,
    2.000 client không cùng lúc ập vào API xin ticket (thundering herd).
    """

    def __init__(self, api: ApiClient, ws_base_url: str, events: "queue.Queue[tuple]",
                 name: str = "realtime-worker") -> None:
        super().__init__(name=name, daemon=True)
        self.api = api
        self.ws_base_url = ws_base_url.rstrip("/")
        self.events = events
        # Cursor Redis Stream. Bắt đầu = "bây giờ" vì lịch sử đã lấy qua REST /feed.
        self.last_event_id = f"{int(time.time() * 1000)}-0"
        self._stop_event = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws: Any = None

    # ---------------- Các điểm mở rộng (JobWatcher ghi đè) ----------------
    def ws_path(self) -> str:
        return "/ws/events"

    def request_ticket(self) -> str:
        return self.api.get_ws_ticket("user")

    def emit_status(self, status: str, message: str = "") -> None:
        self.events.put(("status", status, message))

    def on_connected(self) -> None:
        self.emit_status("connected")
        # Đồng bộ lại hộp thư: bù các thông báo có thể đã lỡ khi mất kết nối lâu.
        self.events.put(("resync", None, None))

    def on_replay_incomplete(self) -> None:
        self.events.put(("resync", None, None))

    def on_notification(self, event: dict[str, Any]) -> None:
        self.events.put(("notification", event, None))

    def is_fatal_api_error(self, exc: ApiError) -> bool:
        """Lỗi API không nên thử lại (ví dụ không có quyền). Kênh user: luôn thử lại."""
        return False

    # ---------------- Vòng đời thread ----------------
    @property
    def stopped(self) -> bool:
        return self._stop_event.is_set()

    def stop(self) -> None:
        self._stop_event.set()
        if self._loop and self._ws is not None:
            try:
                asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
            except RuntimeError:  # loop đã đóng
                pass

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        try:
            self._loop.run_until_complete(self._main())
        finally:
            self._loop.close()
            self.emit_status("stopped")

    async def _sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while not self._stop_event.is_set() and time.monotonic() < end:
            await asyncio.sleep(0.2)

    async def _main(self) -> None:
        failures = 0
        while not self._stop_event.is_set():
            self.emit_status("connecting" if failures == 0 else "reconnecting")
            delay: float | None = None
            try:
                # Xin ticket là gọi HTTP đồng bộ -> chạy trong thread để không chặn event loop.
                ticket = await asyncio.to_thread(self.request_ticket)
                url = f"{self.ws_base_url}{self.ws_path()}?" + urlencode(
                    {"ticket": ticket, "last_event_id": self.last_event_id}
                )
                async with websockets.connect(
                    url, open_timeout=15, ping_interval=20, ping_timeout=20, close_timeout=3,
                ) as ws:
                    self._ws = ws
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        self._handle_message(raw)
                        if failures:
                            failures = 0
                        if self._stop_event.is_set():  # vd JobWatcher: job đã kết thúc
                            break
                self._ws = None
            except ConnectionClosed as exc:
                self._ws = None
                code = exc.rcvd.code if exc.rcvd else None
                reason = exc.rcvd.reason if exc.rcvd else ""
                if self._stop_event.is_set():
                    break
                # Bảng close code: noti_server/src/utils/close_codes.py
                if code in (4400, 4403):
                    self.emit_status("error", f"Bị từ chối ({code} {reason}), không kết nối lại")
                    return
                if code == 4401:          # ticket hết hạn/đã dùng -> xin ticket mới ngay
                    delay = 0.5
                elif code == 1012:        # server đang khởi động lại
                    delay = 1 + random.uniform(0, 4)
                elif code in (1013, 4429):  # server quá tải / quá nhiều tab
                    delay = 15 + random.uniform(0, 15)
            except ApiError as exc:
                self._ws = None
                if self.is_fatal_api_error(exc):
                    self.emit_status("error", str(exc)[:150])
                    return
                self.emit_status("reconnecting", str(exc)[:150])
            except (OSError, InvalidStatus, asyncio.TimeoutError) as exc:
                self._ws = None
                self.emit_status("reconnecting", str(exc)[:150])
            except Exception as exc:  # noqa: BLE001 - không để thread chết
                self._ws = None
                self.emit_status("reconnecting", f"{type(exc).__name__}: {exc}"[:150])

            if self._stop_event.is_set():
                break
            failures += 1
            if delay is None:
                delay = min(30.0, 2 ** min(failures - 1, 5)) + random.uniform(0, 1.5)
            await self._sleep(delay)

    def _handle_message(self, raw: str | bytes) -> None:
        try:
            event = json.loads(raw)
        except ValueError:
            return
        msg_type = event.get("type")
        if msg_type == "connected":
            self.on_connected()
            return
        if msg_type in ("heartbeat", "pong"):
            return
        if msg_type == "replay_incomplete":
            self.on_replay_incomplete()
            return
        stream_id = event.get("stream_id") or event.get("event_id")
        if stream_id:
            self.last_event_id = str(stream_id)
        if event.get("notification_id"):
            self.on_notification(event)


class JobWatcher(RealtimeWorker):
    """
    Nghe stream của MỘT job: ws://<noti>/ws/jobs/{job_id} (ticket channel=job&job_id=...).

    Đẩy vào queue:
        ("job_event", event, job_id)   -> thông báo job.* (started / progress / retrying / ...)
        ("job_sync", job_id, None)     -> vừa kết nối: UI nên lấy trạng thái job qua REST
        ("job_watch_error", job_id, msg) -> không theo dõi được (không có quyền / job không tồn tại)

    Tự dừng khi nhận job.succeeded / job.failed. Đọc stream từ đầu (0-0) để nhận cả các
    bước tiến độ đã diễn ra trước khi kết nối (server giới hạn số event gửi bù).
    """

    TERMINAL_TYPES = {"job.succeeded", "job.failed"}

    def __init__(self, api: ApiClient, ws_base_url: str, events: "queue.Queue[tuple]", job_id: str) -> None:
        super().__init__(api, ws_base_url, events, name=f"job-watcher-{job_id[:8]}")
        self.job_id = job_id
        self.last_event_id = "0-0"

    def ws_path(self) -> str:
        return f"/ws/jobs/{self.job_id}"

    def request_ticket(self) -> str:
        return self.api.get_ws_ticket("job", job_id=self.job_id)

    def emit_status(self, status: str, message: str = "") -> None:
        # Không đổi trạng thái kết nối chính của app; chỉ báo lỗi không thể theo dõi.
        if status == "error":
            self.events.put(("job_watch_error", self.job_id, message))

    def on_connected(self) -> None:
        self.events.put(("job_sync", self.job_id, None))

    def on_replay_incomplete(self) -> None:
        self.events.put(("job_sync", self.job_id, None))

    def on_notification(self, event: dict[str, Any]) -> None:
        self.events.put(("job_event", event, self.job_id))
        if event.get("notification_type") in self.TERMINAL_TYPES:
            self._stop_event.set()

    def is_fatal_api_error(self, exc: ApiError) -> bool:
        return exc.status in (400, 403, 404)


# ============================================================
# GIAO DIỆN
# ============================================================

class LoginFrame(ctk.CTkFrame):
    def __init__(self, master: "NotificationApp", on_login: Callable[[str, str, str, str], None]) -> None:
        super().__init__(master, fg_color="transparent")
        self.on_login = on_login
        settings = load_settings()

        box = ctk.CTkFrame(self, corner_radius=16)
        box.place(relx=0.5, rely=0.5, anchor="center")

        ctk.CTkLabel(box, text="🔔", font=ctk.CTkFont(size=44)).pack(pady=(28, 0))
        ctk.CTkLabel(box, text=APP_TITLE, font=ctk.CTkFont(size=22, weight="bold")).pack(pady=(4, 2))
        ctk.CTkLabel(box, text="Đăng nhập để nhận thông báo realtime",
                     text_color=("gray40", "gray65")).pack(pady=(0, 18))

        self.username = ctk.CTkEntry(box, width=300, height=38, placeholder_text="Tên đăng nhập")
        self.username.pack(padx=32, pady=6)
        self.password = ctk.CTkEntry(box, width=300, height=38, placeholder_text="Mật khẩu", show="•")
        self.password.pack(padx=32, pady=6)
        if settings.get("username"):
            self.username.insert(0, settings["username"])

        # Cấu hình server (ẩn/hiện)
        self.server_box = ctk.CTkFrame(box, fg_color="transparent")
        ctk.CTkLabel(self.server_box, text="API Server", anchor="w").pack(fill="x", padx=32)
        self.api_url = ctk.CTkEntry(self.server_box, width=300)
        self.api_url.insert(0, settings.get("api_url", DEFAULT_API_URL))
        self.api_url.pack(padx=32, pady=(0, 6))
        ctk.CTkLabel(self.server_box, text="Notification Server (WebSocket)", anchor="w").pack(fill="x", padx=32)
        self.ws_url = ctk.CTkEntry(self.server_box, width=300)
        self.ws_url.insert(0, settings.get("ws_url", DEFAULT_WS_URL))
        self.ws_url.pack(padx=32, pady=(0, 6))

        self.toggle_btn = ctk.CTkButton(box, text="⚙ Cấu hình server", width=300, fg_color="transparent",
                                        text_color=("gray30", "gray70"), hover=False, command=self._toggle_server)
        self.toggle_btn.pack(pady=(4, 0))
        self._server_visible = False

        self.error_label = ctk.CTkLabel(box, text="", text_color="#EF4444", wraplength=300)
        self.error_label.pack(padx=32, pady=(6, 0))

        self.login_btn = ctk.CTkButton(box, text="Đăng nhập", width=300, height=40,
                                       font=ctk.CTkFont(size=15, weight="bold"), command=self._submit)
        self.login_btn.pack(padx=32, pady=(8, 28))

        for entry in (self.username, self.password):
            entry.bind("<Return>", lambda _e: self._submit())
        (self.password if settings.get("username") else self.username).focus_set()

    def _toggle_server(self) -> None:
        if self._server_visible:
            self.server_box.pack_forget()
        else:
            self.server_box.pack(before=self.toggle_btn, pady=(8, 0))
        self._server_visible = not self._server_visible

    def _submit(self) -> None:
        username = self.username.get().strip()
        password = self.password.get()
        if not username or not password:
            self.show_error("Hãy nhập tên đăng nhập và mật khẩu")
            return
        self.set_busy(True)
        self.on_login(self.api_url.get().strip(), self.ws_url.get().strip(), username, password)

    def set_busy(self, busy: bool) -> None:
        self.login_btn.configure(state="disabled" if busy else "normal",
                                 text="Đang đăng nhập..." if busy else "Đăng nhập")
        if busy:
            self.error_label.configure(text="")

    def show_error(self, message: str) -> None:
        self.set_busy(False)
        self.error_label.configure(text=message)


class NotificationCard(ctk.CTkFrame):
    def __init__(self, master: Any, item: NotificationItem, on_click: Callable[[str], None]) -> None:
        super().__init__(master, corner_radius=10, border_width=1, height=1)
        self.item = item
        self.on_click = on_click

        color = SEVERITY_COLORS.get(item.severity, SEVERITY_COLORS["info"])
        # height=1: CTkFrame mặc định cao 200px; thanh màu không có widget con nên phải tự đặt nhỏ,
        # pack(fill="y") sẽ kéo dài nó theo chiều cao thật của thẻ.
        self.stripe = ctk.CTkFrame(self, width=5, height=1, corner_radius=3, fg_color=color)
        self.stripe.pack(side="left", fill="y", padx=(6, 0), pady=8)

        content = ctk.CTkFrame(self, fg_color="transparent", height=1)
        content.pack(side="left", fill="both", expand=True, padx=10, pady=8)

        header = ctk.CTkFrame(content, fg_color="transparent", height=1)
        header.pack(fill="x")
        self.dot = ctk.CTkLabel(header, text="●", text_color="#3B82F6", width=14)
        self.title = ctk.CTkLabel(header, text=item.title, anchor="w", justify="left", wraplength=330)
        self.title.pack(side="left", fill="x", expand=True)

        self.body = ctk.CTkLabel(content, text=item.body, anchor="w", justify="left", wraplength=360,
                                 text_color=("gray20", "gray80"))
        if item.body:
            self.body.pack(fill="x", pady=(2, 0))

        meta = format_time(item.created_at)
        if item.notification_type:
            meta = f"{meta}   ·   {item.notification_type}"
        self.meta = ctk.CTkLabel(content, text=meta, anchor="w", font=ctk.CTkFont(size=11),
                                 text_color=("gray45", "gray60"))
        self.meta.pack(fill="x", pady=(2, 0))

        for widget in (self, content, header, self.title, self.body, self.meta, self.stripe):
            widget.bind("<Button-1>", lambda _e: self.on_click(self.item.notification_id))
        self.refresh_state()

    def refresh_state(self) -> None:
        unread = self.item.is_unread
        self.title.configure(font=ctk.CTkFont(size=14, weight="bold" if unread else "normal"))
        self.configure(fg_color=("#EEF4FF", "#1E2A3D") if unread else ("gray95", "gray17"),
                       border_color=("#BFD3FF", "#2F4A73") if unread else ("gray85", "gray25"))
        if unread:
            self.dot.pack(side="left", before=self.title)
        else:
            self.dot.pack_forget()


class JobCard(ctk.CTkFrame):
    """Thẻ 1 job: tên file, trạng thái, thanh tiến trình, thông điệp, nút tải kết quả / ẩn."""

    def __init__(self, master: Any, job: JobState, on_download: Callable[[str], None],
                 on_dismiss: Callable[[str], None]) -> None:
        super().__init__(master, corner_radius=10, border_width=1, height=1,
                         fg_color=("gray97", "gray16"), border_color=("gray85", "gray25"))
        self.job = job

        header = ctk.CTkFrame(self, fg_color="transparent", height=1)
        header.pack(fill="x", padx=10, pady=(8, 0))
        self.name = ctk.CTkLabel(header, text="", anchor="w", font=ctk.CTkFont(size=13, weight="bold"))
        self.name.pack(side="left", fill="x", expand=True)
        ctk.CTkButton(header, text="✕", width=24, height=22, fg_color="transparent", hover=False,
                      text_color=("gray40", "gray60"),
                      command=lambda: on_dismiss(self.job.job_id)).pack(side="right")
        self.status = ctk.CTkLabel(header, text="", font=ctk.CTkFont(size=12, weight="bold"))
        self.status.pack(side="right", padx=(6, 2))

        self.bar = ctk.CTkProgressBar(self, height=8)
        self.bar.pack(fill="x", padx=10, pady=(6, 2))

        footer = ctk.CTkFrame(self, fg_color="transparent", height=1)
        footer.pack(fill="x", padx=10, pady=(0, 8))
        self.message = ctk.CTkLabel(footer, text="", anchor="w", justify="left", wraplength=300,
                                    font=ctk.CTkFont(size=11), text_color=("gray35", "gray65"))
        self.message.pack(side="left", fill="x", expand=True)
        self.download_btn = ctk.CTkButton(footer, text="⬇ Kết quả", width=90, height=24,
                                          command=lambda: on_download(self.job.job_id))
        self.update_view()

    def update_view(self) -> None:
        job = self.job
        label, color = JOB_STATUS_STYLE.get(job.status, (job.status, "#9CA3AF"))
        self.name.configure(text=f"📄 {job.filename or job.job_id[:12]}")
        self.status.configure(text=f"{label} · {job.progress}%" if not job.is_terminal else label,
                              text_color=color)
        self.bar.configure(progress_color=color)
        self.bar.set(max(0, min(100, job.progress)) / 100)
        text = job.error if job.status == "failed" and job.error else job.message
        self.message.configure(text=(text or "")[:160])
        if job.status == "success":
            self.download_btn.pack(side="right", padx=(6, 0))
        else:
            self.download_btn.pack_forget()


class Toast(ctk.CTkToplevel):
    """Popup nhỏ góc dưới bên phải màn hình, tự đóng sau vài giây."""

    def __init__(self, master: "NotificationApp", title: str, body: str, severity: str, index: int) -> None:
        super().__init__(master)
        self.overrideredirect(True)
        self.attributes("-topmost", True)
        width, height = 340, 92
        x = self.winfo_screenwidth() - width - 24
        y = self.winfo_screenheight() - (height + 12) * (index + 1) - 60
        self.geometry(f"{width}x{height}+{x}+{y}")

        frame = ctk.CTkFrame(self, corner_radius=12, border_width=2,
                             border_color=SEVERITY_COLORS.get(severity, SEVERITY_COLORS["info"]))
        frame.pack(fill="both", expand=True)
        ctk.CTkLabel(frame, text=f"🔔  {title}", anchor="w", wraplength=300,
                     font=ctk.CTkFont(size=13, weight="bold")).pack(fill="x", padx=14, pady=(12, 0))
        ctk.CTkLabel(frame, text=body[:140], anchor="w", justify="left", wraplength=310,
                     text_color=("gray25", "gray75")).pack(fill="x", padx=14, pady=(2, 10))
        for widget in (self, frame):
            widget.bind("<Button-1>", lambda _e: self._click(master))
        self.after(int(TOAST_SECONDS * 1000), self.destroy)

    def _click(self, master: "NotificationApp") -> None:
        master.deiconify()
        master.lift()
        self.destroy()


class MainFrame(ctk.CTkFrame):
    def __init__(self, master: "NotificationApp") -> None:
        super().__init__(master, fg_color="transparent")
        self.app = master

        # ---- Thanh trên ----
        top = ctk.CTkFrame(self, corner_radius=0, height=64)
        top.pack(fill="x")
        left = ctk.CTkFrame(top, fg_color="transparent")
        left.pack(side="left", padx=14, pady=8)
        self.user_label = ctk.CTkLabel(left, text="", font=ctk.CTkFont(size=16, weight="bold"), anchor="w")
        self.user_label.pack(anchor="w")
        self.status_label = ctk.CTkLabel(left, text="", font=ctk.CTkFont(size=12), anchor="w")
        self.status_label.pack(anchor="w")

        ctk.CTkButton(top, text="Đăng xuất", width=90, fg_color="transparent", border_width=1,
                      text_color=("gray20", "gray85"), command=master.logout).pack(side="right", padx=(6, 14))
        self.badge = ctk.CTkLabel(top, text="🔔 0", font=ctk.CTkFont(size=15, weight="bold"),
                                  corner_radius=12, fg_color="#EF4444", text_color="white", width=64, height=28)
        self.badge.pack(side="right", padx=6)
        self.upload_btn = ctk.CTkButton(top, text="📤 Tải file", width=90, command=master.upload_file)
        self.upload_btn.pack(side="right", padx=6)

        # ---- Khung công việc (job): chỉ hiện khi có job ----
        self.job_panel = ctk.CTkFrame(self, corner_radius=12)
        job_head = ctk.CTkFrame(self.job_panel, fg_color="transparent", height=1)
        job_head.pack(fill="x", padx=10, pady=(8, 2))
        self.job_title = ctk.CTkLabel(job_head, text="Công việc", anchor="w",
                                      font=ctk.CTkFont(size=14, weight="bold"))
        self.job_title.pack(side="left")
        self.job_list = ctk.CTkFrame(self.job_panel, fg_color="transparent", height=1)
        self.job_list.pack(fill="x", padx=6, pady=(0, 8))
        self.job_cards: dict[str, JobCard] = {}

        # ---- Thanh công cụ ----
        bar = ctk.CTkFrame(self, fg_color="transparent")
        self.toolbar = bar
        bar.pack(fill="x", padx=12, pady=(10, 4))
        self.filter = ctk.CTkSegmentedButton(bar, values=["Tất cả", "Chưa đọc"], command=lambda _v: self.render())
        self.filter.set("Tất cả")
        self.filter.pack(side="left")
        ctk.CTkButton(bar, text="⟳", width=36, command=master.resync).pack(side="right")
        ctk.CTkButton(bar, text="Đánh dấu đã đọc tất cả", width=170,
                      command=master.mark_all_read).pack(side="right", padx=6)

        # ---- Danh sách ----
        self.list = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.list.pack(fill="both", expand=True, padx=8, pady=(4, 8))
        self.empty_label = ctk.CTkLabel(self.list, text="Chưa có thông báo nào",
                                        text_color=("gray50", "gray60"))
        self.cards: dict[str, NotificationCard] = {}

    # ---------------- Hiển thị ----------------
    def set_user(self, text: str) -> None:
        self.user_label.configure(text=text)

    def set_status(self, status: str, detail: str = "") -> None:
        text, color = STATUS_STYLE.get(status, (status, "gray"))
        if detail and status != "connected":
            text = f"{text}  ({detail[:60]})"
        self.status_label.configure(text=text, text_color=color)

    def set_unread(self, count: int) -> None:
        self.badge.configure(text=f"🔔 {count}", fg_color="#EF4444" if count else ("gray70", "gray30"))

    def _visible(self, item: NotificationItem) -> bool:
        return self.filter.get() == "Tất cả" or item.is_unread

    def render(self) -> None:
        """Vẽ lại toàn bộ danh sách (khi đồng bộ / đổi bộ lọc). Tối đa MAX_CARDS thẻ."""
        for card in self.cards.values():
            card.destroy()
        self.cards.clear()
        items = [i for i in self.app.ordered_items() if self._visible(i)][:MAX_CARDS]
        for item in items:
            card = NotificationCard(self.list, item, self.app.mark_read)
            card.pack(fill="x", padx=4, pady=4)
            self.cards[item.notification_id] = card
        if items:
            self.empty_label.pack_forget()
        else:
            self.empty_label.pack(pady=40)

    def add_on_top(self, item: NotificationItem) -> None:
        """Thêm 1 thẻ mới lên đầu danh sách (thông báo realtime), không vẽ lại cả danh sách."""
        if item.notification_id in self.cards or not self._visible(item):
            return
        self.empty_label.pack_forget()
        card = NotificationCard(self.list, item, self.app.mark_read)
        first = next(iter(self.cards.values()), None)
        if first is not None:
            card.pack(fill="x", padx=4, pady=4, before=first)
        else:
            card.pack(fill="x", padx=4, pady=4)
        # Giữ dict theo thứ tự hiển thị: thẻ mới ở đầu.
        self.cards = {item.notification_id: card, **self.cards}
        while len(self.cards) > MAX_CARDS:
            _, oldest = self.cards.popitem()
            oldest.destroy()

    # ---------------- Công việc (job) ----------------
    def render_jobs(self) -> None:
        """Vẽ lại khung công việc: tối đa MAX_JOB_CARDS job (job đang chạy lên trước)."""
        jobs = self.app.visible_jobs()[:MAX_JOB_CARDS]
        wanted = [j.job_id for j in jobs]
        if list(self.job_cards) != wanted:
            for card in self.job_cards.values():
                card.destroy()
            self.job_cards = {}
            for job in jobs:
                card = JobCard(self.job_list, job, self.app.download_job_result, self.app.dismiss_job)
                card.pack(fill="x", padx=4, pady=3)
                self.job_cards[job.job_id] = card
        else:
            for card in self.job_cards.values():
                card.update_view()

        running = sum(1 for j in self.app.jobs.values() if not j.is_terminal)
        self.job_title.configure(text=f"Công việc  ·  {running} đang chạy" if running else "Công việc")
        if jobs:
            self.job_panel.pack(fill="x", padx=12, pady=(10, 0), before=self.toolbar)
        else:
            self.job_panel.pack_forget()

    def update_job(self, job_id: str) -> None:
        """Cập nhật 1 thẻ job (không vẽ lại cả khung) nếu thẻ đang hiển thị."""
        card = self.job_cards.get(job_id)
        if card is None:
            self.render_jobs()
            return
        card.update_view()
        running = sum(1 for j in self.app.jobs.values() if not j.is_terminal)
        self.job_title.configure(text=f"Công việc  ·  {running} đang chạy" if running else "Công việc")

    def set_uploading(self, busy: bool) -> None:
        self.upload_btn.configure(state="disabled" if busy else "normal",
                                  text="Đang tải..." if busy else "📤 Tải file")

    def refresh_card(self, notification_id: str) -> None:
        card = self.cards.get(notification_id)
        if card is None:
            return
        if self.filter.get() == "Chưa đọc" and not card.item.is_unread:
            card.destroy()
            self.cards.pop(notification_id, None)
        else:
            card.refresh_state()


class NotificationApp(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        ctk.set_appearance_mode("system")
        ctk.set_default_color_theme("blue")
        self.title(APP_TITLE)
        self.geometry("520x800")
        self.minsize(460, 560)

        self.events: "queue.Queue[tuple]" = queue.Queue()
        self.api: ApiClient | None = None
        self.worker: RealtimeWorker | None = None
        self.items: dict[str, NotificationItem] = {}
        self.unread_count = 0
        self._toasts: list[Toast] = []
        self._pending_toasts: list[NotificationItem] = []
        self.ws_url = DEFAULT_WS_URL
        self.jobs: dict[str, JobState] = {}
        self.hidden_jobs: set[str] = set()
        self.job_watchers: dict[str, JobWatcher] = {}

        self.login_frame = LoginFrame(self, self.login)
        self.main_frame = MainFrame(self)
        self.login_frame.pack(fill="both", expand=True)

        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.after(UI_POLL_MS, self._poll_events)

    # ---------------- Chạy việc nền ----------------
    def run_bg(self, func: Callable[[], Any], on_done: Callable[[Any], None] | None = None,
               on_error: Callable[[Exception], None] | None = None) -> None:
        """Chạy func trong thread nền; kết quả quay về thread giao diện qua queue."""
        def target() -> None:
            try:
                result = func()
            except Exception as exc:  # noqa: BLE001
                self.events.put(("callback", on_error, exc) if on_error else ("error", str(exc), None))
                return
            if on_done:
                self.events.put(("callback", on_done, result))
        threading.Thread(target=target, daemon=True).start()

    # ---------------- Đăng nhập / đăng xuất ----------------
    def login(self, api_url: str, ws_url: str, username: str, password: str) -> None:
        api = ApiClient(api_url or DEFAULT_API_URL)

        def on_done(profile: dict[str, Any]) -> None:
            save_settings({"api_url": api.base_url, "ws_url": ws_url, "username": username})
            self.api = api
            self.ws_url = ws_url or DEFAULT_WS_URL
            self.items.clear()
            self.jobs.clear()
            self.hidden_jobs.clear()
            self.login_frame.pack_forget()
            self.login_frame.password.delete(0, "end")
            self.login_frame.set_busy(False)
            self.main_frame.pack(fill="both", expand=True)
            name = profile.get("user_name") or username
            self.main_frame.set_user(f"Xin chào, {name}")
            self.main_frame.set_unread(0)
            self.main_frame.render()
            self.resync()
            self.load_jobs()
            self.worker = RealtimeWorker(api, self.ws_url, self.events)
            self.worker.start()

        def on_error(exc: Exception) -> None:
            api.close()
            self.login_frame.show_error(str(exc))

        self.run_bg(lambda: api.login(username, password), on_done, on_error)

    def logout(self) -> None:
        self.stop_job_watchers()
        if self.worker:
            self.worker.stop()
            self.worker = None
        if self.api:
            self.api.close()
            self.api = None
        self.items.clear()
        self.jobs.clear()
        self.main_frame.render_jobs()
        self.main_frame.pack_forget()
        self.login_frame.pack(fill="both", expand=True)

    def on_close(self) -> None:
        self.stop_job_watchers()
        if self.worker:
            self.worker.stop()
        self.destroy()

    # ---------------- Dữ liệu ----------------
    def ordered_items(self) -> list[NotificationItem]:
        return sorted(self.items.values(), key=lambda i: i.created_at, reverse=True)

    def resync(self) -> None:
        """Tải lại hộp thư từ REST (nguồn sự thật là SQL Server)."""
        api = self.api
        if api is None:
            return

        def on_done(result: tuple[list[NotificationItem], int]) -> None:
            if self.api is not api:
                return
            feed, unread = result
            for item in feed:
                self.items[item.notification_id] = item
            # Giới hạn bộ nhớ: chỉ giữ MAX_CARDS thông báo mới nhất.
            for item in self.ordered_items()[MAX_CARDS:]:
                self.items.pop(item.notification_id, None)
            self.unread_count = unread
            self.main_frame.set_unread(unread)
            self.main_frame.render()

        self.run_bg(api.get_feed, on_done)

    def mark_read(self, notification_id: str) -> None:
        item = self.items.get(notification_id)
        if item is None or not item.is_unread or self.api is None:
            return
        item.state = "read"
        self.unread_count = max(0, self.unread_count - 1)
        self.main_frame.set_unread(self.unread_count)
        self.main_frame.refresh_card(notification_id)
        api = self.api
        self.run_bg(lambda: api.mark_read(notification_id))

    def mark_all_read(self) -> None:
        if self.api is None:
            return
        for item in self.items.values():
            item.state = "read"
        self.unread_count = 0
        self.main_frame.set_unread(0)
        self.main_frame.render()
        api = self.api
        self.run_bg(api.mark_all_read)

    # ---------------- Công việc (job) ----------------
    def visible_jobs(self) -> list[JobState]:
        """Job hiển thị: chưa bị ẩn; job đang chạy lên trước, rồi mới nhất trước."""
        jobs = [j for j in self.jobs.values() if j.job_id not in self.hidden_jobs]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        jobs.sort(key=lambda j: j.is_terminal)  # sort ổn định: giữ thứ tự thời gian trong mỗi nhóm
        return jobs

    def load_jobs(self) -> None:
        """Tải vài job gần nhất khi đăng nhập; job chưa xong thì theo dõi tiếp."""
        api = self.api
        if api is None:
            return

        def on_done(rows: list[dict[str, Any]]) -> None:
            if self.api is not api:
                return
            for row in rows:
                self.upsert_job_from_rest(row)
            self.main_frame.render_jobs()

        # Không có quyền xem job / server cũ chưa có /jobs -> bỏ qua, không làm phiền người dùng.
        self.run_bg(lambda: api.list_jobs(JOB_RECENT_LIMIT), on_done, lambda _exc: None)

    def upsert_job_from_rest(self, row: dict[str, Any]) -> JobState:
        job_id = str(row["job_id"])
        job = self.jobs.get(job_id) or JobState(job_id=job_id)
        job.apply_rest(row)
        self.jobs[job_id] = job
        self.after_job_changed(job)
        return job

    def after_job_changed(self, job: JobState) -> None:
        """Job chưa xong -> mở WebSocket theo dõi; xong rồi -> đóng WebSocket."""
        watcher = self.job_watchers.get(job.job_id)
        if job.is_terminal:
            if watcher:
                watcher.stop()
                self.job_watchers.pop(job.job_id, None)
            return
        if watcher and not watcher.stopped and watcher.is_alive():
            return
        if self.api is None or len(self.job_watchers) >= MAX_JOB_WATCHERS:
            return  # vẫn nhận job.succeeded / job.failed qua kênh user
        watcher = JobWatcher(self.api, self.ws_url, self.events, job.job_id)
        self.job_watchers[job.job_id] = watcher
        watcher.start()

    def stop_job_watchers(self) -> None:
        for watcher in self.job_watchers.values():
            watcher.stop()
        self.job_watchers.clear()

    def handle_job_event(self, event: dict[str, Any]) -> None:
        """Thông báo job.* (từ kênh user hoặc kênh job) -> cập nhật thẻ job."""
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        job_id = str(data.get("job_id") or "")
        if not job_id:
            return
        job = self.jobs.get(job_id)
        if job is None:
            # Job tạo từ nơi khác (CLI, máy khác) -> vẫn hiện và theo dõi.
            job = JobState(job_id=job_id, created_at=str(event.get("created_at") or ""))
            self.jobs[job_id] = job
        if job.apply_event(event):
            self.after_job_changed(job)
            self.main_frame.update_job(job_id)

    def sync_job(self, job_id: str) -> None:
        """Lấy trạng thái job qua REST (khi WebSocket job vừa kết nối, bù các event đã lỡ)."""
        api = self.api
        if api is None:
            return

        def on_done(row: dict[str, Any]) -> None:
            if self.api is api:
                self.upsert_job_from_rest(row)
                self.main_frame.update_job(job_id)

        self.run_bg(lambda: api.get_job(job_id), on_done, lambda _exc: None)

    def upload_file(self) -> None:
        api = self.api
        if api is None:
            return
        path = filedialog.askopenfilename(title="Chọn file cần xử lý", parent=self)
        if not path:
            return
        self.main_frame.set_uploading(True)

        def on_done(row: dict[str, Any]) -> None:
            self.main_frame.set_uploading(False)
            if self.api is not api:
                return
            self.hidden_jobs.discard(str(row["job_id"]))
            self.upsert_job_from_rest(row)
            self.main_frame.render_jobs()

        def on_error(exc: Exception) -> None:
            self.main_frame.set_uploading(False)
            self._show_toast("Tải file thất bại", str(exc), "error")

        self.run_bg(lambda: api.upload_job(path), on_done, on_error)

    def download_job_result(self, job_id: str) -> None:
        api = self.api
        job = self.jobs.get(job_id)
        if api is None or job is None:
            return
        stem = (job.filename or job_id).rsplit(".", 1)[0]
        destination = filedialog.asksaveasfilename(
            title="Lưu kết quả", parent=self, initialfile=f"{stem}_result.json",
            defaultextension=".json", filetypes=[("JSON", "*.json"), ("Tất cả", "*.*")],
        )
        if not destination:
            return
        self.run_bg(
            lambda: api.download_result(job_id, destination),
            lambda path: self._show_toast("Đã tải kết quả", str(path), "success"),
            lambda exc: self._show_toast("Không tải được kết quả", str(exc), "error"),
        )

    def dismiss_job(self, job_id: str) -> None:
        """Ẩn thẻ job khỏi khung (job đang chạy vẫn được theo dõi, chỉ không hiển thị)."""
        self.hidden_jobs.add(job_id)
        self.main_frame.render_jobs()

    # ---------------- Vòng lặp sự kiện ----------------
    def _poll_events(self) -> None:
        processed = 0
        new_items: list[NotificationItem] = []
        try:
            while processed < UI_EVENTS_PER_TICK:
                kind, payload, extra = self.events.get_nowait()
                processed += 1
                if kind == "status":
                    self.main_frame.set_status(payload, extra or "")
                elif kind == "resync":
                    self.resync()
                elif kind == "notification":
                    if str(payload.get("notification_type") or "").startswith("job."):
                        self.handle_job_event(payload)
                    item = NotificationItem.from_realtime(payload)
                    if item and item.notification_id not in self.items:  # chống trùng
                        self.items[item.notification_id] = item
                        new_items.append(item)
                elif kind == "job_event":
                    # Event từ kênh job: chỉ cập nhật thanh tiến trình, không vào hộp thư / popup.
                    self.handle_job_event(payload)
                elif kind == "job_sync":
                    self.sync_job(payload)
                elif kind == "job_watch_error":
                    self.job_watchers.pop(payload, None)
                    job = self.jobs.get(payload)
                    if job is not None and not job.is_terminal:
                        job.message = f"Không theo dõi realtime được: {extra}"
                        self.main_frame.update_job(payload)
                elif kind == "callback":
                    if payload:
                        payload(extra)
                elif kind == "error":
                    self.main_frame.set_status("error", str(payload))
        except queue.Empty:
            pass

        if new_items:
            for item in new_items:
                self.main_frame.add_on_top(item)
            self.unread_count += len(new_items)
            self.main_frame.set_unread(self.unread_count)
            self._notify(new_items)

        self.after(UI_POLL_MS, self._poll_events)

    def _notify(self, items: list[NotificationItem]) -> None:
        """Hiện popup; nhận dồn dập nhiều thông báo thì gộp thành 1 popup tóm tắt."""
        self._toasts = [t for t in self._toasts if t.winfo_exists()]
        if len(items) > 2:
            self._show_toast(f"Bạn có {len(items)} thông báo mới", items[0].title, items[0].severity)
            return
        for item in items:
            self._show_toast(item.title, item.body, item.severity)

    def _show_toast(self, title: str, body: str, severity: str) -> None:
        self._toasts = [t for t in self._toasts if t.winfo_exists()]
        if len(self._toasts) >= MAX_TOASTS:
            self._toasts.pop(0).destroy()
        self._toasts.append(Toast(self, title, body, severity, len(self._toasts)))
        self.bell()


if __name__ == "__main__":
    NotificationApp().mainloop()
