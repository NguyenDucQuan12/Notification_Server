"""
Bước xử lý file của Job Worker.

Đây là bản DEMO: đọc file, tính kích thước, SHA-256, số dòng/số từ (nếu là văn bản)
và ghi kết quả ra JSON. Mỗi giai đoạn nghỉ JOB_DEMO_STEP_SECONDS giây để quan sát được
thông báo tiến độ trên client.

Muốn xử lý thật (OCR, đọc Excel, nhận dạng ảnh...): viết lại process_file() và giữ nguyên
chữ ký hàm. Gọi `await report(phần_trăm, "mô tả")` ở các mốc quan trọng; worker lo phần
cập nhật DB và gửi thông báo. Raise exception khi lỗi; worker lo phần thử lại / đánh dấu failed.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from utils.utils import utc_now

# Hàm báo tiến độ do worker truyền vào: report(progress 0..100, message).
ProgressReporter = Callable[[int, str], Awaitable[None]]

# Thời gian nghỉ giả lập cho mỗi giai đoạn (giây). Đặt 0 khi xử lý thật.
JOB_DEMO_STEP_SECONDS = float(os.getenv("JOB_DEMO_STEP_SECONDS", "1"))

# Với file văn bản nhỏ hơn ngưỡng này thì đếm dòng / từ.
_TEXT_ANALYZE_MAX_BYTES = 20 * 1024 * 1024
_CHUNK = 1024 * 1024


class JobProcessingError(Exception):
    """Lỗi nghiệp vụ không nên thử lại (file hỏng, sai định dạng...)."""


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(_CHUNK):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _analyze_text(path: Path) -> dict[str, Any] | None:
    if path.stat().st_size > _TEXT_ANALYZE_MAX_BYTES:
        return None
    raw = path.read_bytes()
    if b"\x00" in raw[:8192]:
        return None  # file nhị phân
    for encoding in ("utf-8", "utf-16", "cp1258"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        return None
    return {
        "encoding": encoding,
        "line_count": text.count("\n") + (1 if text and not text.endswith("\n") else 0),
        "word_count": len(text.split()),
        "char_count": len(text),
    }


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)  # ghi nguyên tử: client không bao giờ đọc được file kết quả dở dang


async def process_file(
    *,
    job: dict[str, Any],
    input_path: Path,
    output_path: Path,
    report: ProgressReporter,
) -> dict[str, Any]:
    """
    Xử lý 1 file và ghi kết quả vào output_path. Trả về tóm tắt kết quả (đưa vào thông báo).
    """
    if not input_path.is_file():
        raise JobProcessingError(f"Không tìm thấy file upload: {input_path.name}")

    await report(10, "Đang đọc file")
    await asyncio.sleep(JOB_DEMO_STEP_SECONDS)

    size, sha256 = await asyncio.to_thread(_hash_file, input_path)
    await report(40, "Đã tính mã băm SHA-256")
    await asyncio.sleep(JOB_DEMO_STEP_SECONDS)

    text_stats = await asyncio.to_thread(_analyze_text, input_path)
    await report(70, "Đã phân tích nội dung")
    await asyncio.sleep(JOB_DEMO_STEP_SECONDS)

    result = {
        "job_id": job["job_id"],
        "filename": job["filename"],
        "size_bytes": size,
        "sha256": sha256,
        "is_text": text_stats is not None,
        "text_stats": text_stats,
        "processed_at": utc_now().isoformat(),
    }
    await asyncio.to_thread(_write_json, output_path, result)
    await report(90, "Đã ghi file kết quả")

    return {"size_bytes": size, "sha256": sha256, "is_text": text_stats is not None}
