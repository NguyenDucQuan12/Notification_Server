from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx


# ============================================================
# CẤU HÌNH
# ============================================================

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

LOGIN_PATH = os.getenv("LOGIN_PATH", "/auth/login")
CREATE_ROLE_PATH = os.getenv("CREATE_ROLE_PATH", "/roles/create")
UPLOAD_FILE_PATH = os.getenv("UPLOAD_FILE_PATH", "/jobs/upload")
SEND_NOTIFICATION_PATH = os.getenv("SEND_NOTIFICATION_PATH", "/notifications/send")


# ======================
# THÔNG TIN ĐĂNG NHẬP (đổi bằng biến môi trường NOTI_USERNAME / NOTI_PASSWORD)
# ======================
TENANT_ID = os.getenv("TENANT_ID", "system")
USERNAME = os.getenv("NOTI_USERNAME", "quan")
PASSWORD = os.getenv("NOTI_PASSWORD", "123456789")
# Nếu RoleCreate của bạn nhận permissions_json là string JSON thì để True.
# Nếu schema nhận list trực tiếp thì đổi thành False.
ROLE_PERMISSIONS_AS_JSON_STRING = True

# Tên field file trong API upload.
# Nếu backend nhận tham số là UploadFile = File(...), tên biến là "file"
# thì để "file".
UPLOAD_FILE_FIELD_NAME = os.getenv("UPLOAD_FILE_FIELD_NAME", "file")


# ============================================================
# HELPER
# ============================================================

def print_json(title: str, data: Any) -> None:
    print("=" * 80)
    print(title)
    print(json.dumps(data, ensure_ascii=False, indent=2))
    print("=" * 80)


def print_response_error(response: httpx.Response, title: str) -> None:
    print("=" * 80)
    print(title)
    print("Status:", response.status_code)
    print("URL:", response.request.method, response.request.url)
    print("Response text:", response.text)
    print("=" * 80)


def extract_access_token(response_json: dict[str, Any]) -> str:
    """
    Lấy access_token từ response login.

    Định dạng response từ server:

    {
      "success": true,
      "data": {
        "token": {
          "access_token": "...",
          "token_type": "bearer"
        }
      }
    }
    """

    if "access_token" in response_json:
        return str(response_json["access_token"])

    data = response_json.get("data")

    if isinstance(data, dict):
        token_data = data.get("token")

        if isinstance(token_data, dict):
            access_token = token_data.get("access_token")

            if access_token:
                return str(access_token)

        if data.get("access_token"):
            return str(data["access_token"])

    raise ValueError(f"Không tìm thấy access_token trong response: {response_json}")


# ============================================================
# LOGIN
# ============================================================

async def login_and_get_token() -> str:
    """
    Gọi API /auth/login để lấy access_token.
    """

    url = f"{API_BASE_URL}{LOGIN_PATH}"

    # API /auth/login nhận dữ liệu dạng FORM (OAuth2 form, để đăng nhập được từ Swagger),
    # KHÔNG nhận JSON. Gửi json= sẽ bị 422 Unprocessable Entity.
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(
            url,
            data=payload,
        )

        if response.status_code != 200:
            print_response_error(response, "Login failed")

        response.raise_for_status()

        response_json = response.json()

        token = extract_access_token(response_json)

        print("Login thành công.")

        return token


# ============================================================
# TEST API TẠO ROLE
# ============================================================

async def create_role_api(
    *,
    access_token: str,
    role_id: str,
    role_name: str,
    description: str | None = None,
    permissions: list[str] | None = None,
) -> dict[str, Any]:
    """
    Gọi API POST /roles/create để tạo role mới.
    """

    url = f"{API_BASE_URL}{CREATE_ROLE_PATH}"

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    permissions = permissions or []

    # payload PHẢI là dict, không được json.dumps toàn bộ payload.
    payload = {
        "role_id": role_id,
        "role_name": role_name,
        "description": description,
        "permissions_json": json.dumps(permissions, ensure_ascii=False),
    }

    print("Payload type:", type(payload))
    print("Payload:", payload)

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            url,
            headers=headers,
            json=payload,  # Đúng: truyền dict vào json=
        )

        if response.status_code not in (200, 201):
            print_response_error(response, "Create role failed")

        response.raise_for_status()

        response_json = response.json()

        print_json("Create role response", response_json)

        return response_json

async def test_create_role() -> None:
    """
    Test nhanh API tạo role.

    Mỗi lần chạy sẽ tự tạo role_id khác nhau để tránh trùng unique.
    """

    access_token = await login_and_get_token()

    suffix = int(time.time())

    await create_role_api(
        access_token=access_token,
        role_id=f"test_role_{suffix}",
        role_name=f"Test Role {suffix}",
        description="Role test được tạo từ file test_api_actions.py",
        permissions=[
            "notification:subscribe",
            "job:read",
            "job:create",
        ],
    )


# ============================================================
# TEST API UPLOAD FILE
# ============================================================

async def upload_file_api(
    *,
    access_token: str,
    file_path: str,
    upload_path: str = UPLOAD_FILE_PATH,
    extra_data: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Gọi API upload file bằng multipart/form-data.

    Lưu ý:
    - Nếu backend nhận file với tên field khác "file",
      hãy sửa UPLOAD_FILE_FIELD_NAME.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    url = f"{API_BASE_URL}{upload_path}"

    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    data = extra_data or {}

    async with httpx.AsyncClient(timeout=120.0) as client:
        with path.open("rb") as f:
            files = {
                UPLOAD_FILE_FIELD_NAME: (
                    path.name,
                    f,
                    "application/octet-stream",
                )
            }

            response = await client.post(
                url,
                headers=headers,
                data=data,
                files=files,
            )

        if response.status_code not in (200, 201):
            print_response_error(response, "Upload file failed")

        response.raise_for_status()

        try:
            response_json = response.json()
        except Exception:
            response_json = {
                "success": True,
                "data": response.text,
                "message": "Upload thành công nhưng response không phải JSON",
            }

        print_json("Upload file response", response_json)

        return response_json


async def test_upload_file(file_path: str) -> None:
    """
    Test upload một file.
    """

    access_token = await login_and_get_token()

    response_json = await upload_file_api(
        access_token=access_token,
        file_path=file_path,
        upload_path=UPLOAD_FILE_PATH,
        extra_data={
            # Nếu API upload của bạn cần thêm field, thêm ở đây.
            # Ví dụ:
            # "job_type": "excel_process",
            # "tenant_id": TENANT_ID,
        },
    )

    job_id = ((response_json.get("data") or {}).get("job") or {}).get("job_id")
    if job_id:
        print("\nTheo dõi tiến trình job (chạy ở terminal khác):")
        print(f"  python client\\listen_noti_server.py --channel job --job {job_id}")
        print(f"Kết quả khi xong: GET {API_BASE_URL}/jobs/{job_id}/result")


# ============================================================
# GỬI THÔNG BÁO THỬ (POST /notifications/send)
# ============================================================

async def send_notification_api(args: argparse.Namespace) -> None:
    """
    Gửi thông báo qua luồng chuẩn: API -> SQL (outbox) -> Redis Stream -> Notification Server -> client.

    Không truyền --to: gửi cho chính tài khoản đang đăng nhập (client nghe channel "user" sẽ nhận).
    """

    access_token = await login_and_get_token()

    payload: dict[str, Any] = {
        "title": args.title,
        "body": args.body,
        "target_user_ids": args.to or [],
        "send_to_tenant_dashboard": args.dashboard,
        "send_to_global": args.global_stream,
        "job_id": args.job,
        "severity": args.severity,
        "notification_type": args.type,
        "mode": args.mode,
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        for i in range(1, args.count + 1):
            body = dict(payload)
            if args.count > 1:
                body["title"] = f"{args.title} #{i}"

            response = await client.post(
                f"{API_BASE_URL}{SEND_NOTIFICATION_PATH}",
                headers={"Authorization": f"Bearer {access_token}"},
                json=body,
            )

            if response.status_code != 200:
                print_response_error(response, "Send notification failed")
                response.raise_for_status()

            data = response.json().get("data", {})
            publish = data.get("publish") or {}
            print(
                f"[{i}/{args.count}] Đã gửi: notification_id={data.get('notification_id')} "
                f"streams={data.get('stream_keys')} "
                f"published={publish.get('published_count', '(worker)')}"
            )

            if i < args.count:
                await asyncio.sleep(args.interval)


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test gọi API: gửi thông báo, tạo role, upload file.",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # Lệnh gửi thông báo thử
    notify_parser = subparsers.add_parser(
        "notify",
        help="Gửi thông báo qua API POST /notifications/send.",
    )
    notify_parser.add_argument("--title", default="Thông báo thử nghiệm")
    notify_parser.add_argument("--body", default="Xin chào từ API Server!")
    notify_parser.add_argument("--count", type=int, default=1, help="Số thông báo gửi liên tiếp.")
    notify_parser.add_argument("--interval", type=float, default=1.0, help="Giây giữa 2 thông báo.")
    notify_parser.add_argument(
        "--to", nargs="*", metavar="USER_ID",
        help="user_id nhận (cần quyền notification:send). Bỏ trống = gửi cho chính mình.",
    )
    notify_parser.add_argument("--dashboard", action="store_true", help="Gửi thêm vào tenant dashboard stream.")
    notify_parser.add_argument("--global", dest="global_stream", action="store_true", help="Gửi thêm vào global stream.")
    notify_parser.add_argument("--job", default=None, help="Gửi thêm vào stream của job này.")
    notify_parser.add_argument(
        "--severity", default="info", choices=["info", "success", "warning", "error", "critical"],
    )
    notify_parser.add_argument("--type", default="system.message", help="notification_type, dạng a.b")
    notify_parser.add_argument(
        "--mode", default="direct", choices=["direct", "worker"],
        help="direct: API publish Redis ngay; worker: chỉ ghi outbox, Outbox Worker publish.",
    )

    # Lệnh test tạo role
    subparsers.add_parser(
        "create-role",
        help="Gọi API tạo role mới.",
    )

    # Lệnh upload file
    upload_parser = subparsers.add_parser(
        "upload-file",
        help="Gọi API upload file.",
    )

    upload_parser.add_argument(
        "--file",
        required=True,
        help="Đường dẫn file cần upload.",
    )

    return parser


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "notify":
        await send_notification_api(args)

    elif args.command == "create-role":
        await test_create_role()

    elif args.command == "upload-file":
        await test_upload_file(args.file)

    else:
        raise ValueError(f"Command không hợp lệ: {args.command}")


if __name__ == "__main__":
    asyncio.run(main())
    # Gửi 5 thông báo, mỗi cái cách nhau 2 giây, mức cảnh báo
    # python client\client_call_api.py notify --count 5 --interval 2 --severity warning
