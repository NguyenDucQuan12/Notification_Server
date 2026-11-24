"""
Lưu trữ file của luồng job (local disk).

Cấu trúc thư mục (gốc là UPLOAD_DIRECTORY):

    <UPLOAD_DIRECTORY>/<tenant_id>/<job_id>/input/<tên file đã làm sạch>
    <UPLOAD_DIRECTORY>/<tenant_id>/<job_id>/result/result.json

DB (job_record) chỉ lưu "object key" tương đối (ví dụ "demo/abc123/input/report.pdf"),
không lưu đường dẫn tuyệt đối. Muốn chuyển sang S3/MinIO chỉ cần viết lại các hàm trong file này,
API và Job Worker giữ nguyên.

API Server và Job Worker PHẢI dùng chung UPLOAD_DIRECTORY (cùng ổ đĩa / cùng volume Docker).
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
from pathlib import Path
from typing import BinaryIO

from config.config import settings

# Thư mục fastapi_server/src: đường dẫn tương đối trong UPLOAD_DIRECTORY được tính từ đây
# (khớp với ghi chú trong env/.env.api), nên API và worker chạy từ thư mục nào cũng ra cùng 1 chỗ.
_SRC_DIR = Path(__file__).resolve().parents[1]

# Dung lượng file upload tối đa (MB).
JOB_MAX_UPLOAD_MB = int(os.getenv("JOB_MAX_UPLOAD_MB", "50"))
JOB_MAX_UPLOAD_BYTES = JOB_MAX_UPLOAD_MB * 1024 * 1024

_CHUNK_SIZE = 1024 * 1024
_UNSAFE_CHARS = re.compile(r"[^\w.\- ()]+", re.UNICODE)


class UploadTooLargeError(Exception):
    """File upload vượt quá JOB_MAX_UPLOAD_MB."""


def storage_root() -> Path:
    """Thư mục gốc lưu file (tuyệt đối)."""
    root = Path(settings.upload_directory)
    if not root.is_absolute():
        root = _SRC_DIR / root
    return root.resolve()


def sanitize_filename(filename: str | None) -> str:
    """
    Làm sạch tên file người dùng gửi lên:
    - bỏ phần thư mục (chống path traversal kiểu "../../etc/passwd")
    - thay ký tự lạ bằng "_"
    - giới hạn 200 ký tự, giữ đuôi file
    """
    name = Path((filename or "").replace("\\", "/")).name.strip()
    name = _UNSAFE_CHARS.sub("_", name).strip(" .")
    if not name:
        name = "upload.bin"
    if len(name) > 200:
        stem, dot, ext = name.rpartition(".")
        name = (stem[: 200 - len(ext) - 1] + dot + ext) if dot and len(ext) <= 20 else name[:200]
    return name


def upload_object_key(tenant_id: str, job_id: str, filename: str) -> str:
    """Object key của file upload."""
    return f"{tenant_id}/{job_id}/input/{filename}"


def result_object_key(tenant_id: str, job_id: str) -> str:
    """Object key của file kết quả."""
    return f"{tenant_id}/{job_id}/result/result.json"


def resolve_object_path(object_key: str) -> Path:
    """
    Chuyển object key thành đường dẫn tuyệt đối, và chặn mọi key trỏ ra ngoài storage_root().
    """
    root = storage_root()
    path = (root / object_key).resolve()
    if root != path and root not in path.parents:
        raise ValueError(f"object key không hợp lệ: {object_key}")
    return path


def _copy_limited(source: BinaryIO, destination: Path, max_bytes: int) -> int:
    """Chép stream vào file theo từng chunk, dừng khi vượt max_bytes. Chạy trong thread."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with destination.open("wb") as out:
        while True:
            chunk = source.read(_CHUNK_SIZE)
            if not chunk:
                break
            written += len(chunk)
            if written > max_bytes:
                raise UploadTooLargeError(
                    f"File vượt quá dung lượng cho phép ({JOB_MAX_UPLOAD_MB} MB)"
                )
            out.write(chunk)
    return written


async def save_upload(source: BinaryIO, object_key: str) -> int:
    """
    Lưu file upload (UploadFile.file) vào storage. Trả về số byte đã ghi.
    Ghi file chạy trong thread để không chặn event loop. Lỗi giữa chừng -> xóa file dở dang.
    """
    destination = resolve_object_path(object_key)
    try:
        return await asyncio.to_thread(_copy_limited, source, destination, JOB_MAX_UPLOAD_BYTES)
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


async def delete_job_files(tenant_id: str, job_id: str) -> None:
    """Xóa toàn bộ file của 1 job (dùng khi tạo job thất bại hoặc xóa job)."""
    job_dir = resolve_object_path(f"{tenant_id}/{job_id}")
    await asyncio.to_thread(shutil.rmtree, job_dir, True)
