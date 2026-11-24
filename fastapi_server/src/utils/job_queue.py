"""
Hàng đợi job trên Redis List.

    API Server  --LPUSH queue:jobs {"tenant_id","job_id"}-->  Redis  --BRPOP-->  Job Worker

Hàng đợi chỉ là "tín hiệu có việc". Nguồn sự thật là bảng job_record trong SQL Server:
- Mất phần tử trong Redis (Redis restart, LPUSH lỗi) -> worker tự tìm job 'queued' quá hạn và đẩy lại.
- Đẩy trùng 1 job -> chỉ 1 worker claim được (UPDATE ... WHERE status='queued').
"""
from __future__ import annotations

import json
from typing import Any

from services.redis_client import get_redis_fast, get_redis_stream
from utils.redis_keys import JOB_QUEUE_KEY


async def enqueue_job(*, tenant_id: str, job_id: str) -> None:
    """Đẩy 1 job vào cuối hàng đợi. Raise nếu Redis lỗi (caller tự quyết định xử lý)."""
    message = json.dumps({"tenant_id": tenant_id, "job_id": job_id})
    await get_redis_fast().lpush(JOB_QUEUE_KEY, message)


async def dequeue_job(timeout_seconds: float) -> dict[str, Any] | None:
    """
    Chờ tối đa timeout_seconds để lấy 1 job (BRPOP, FIFO vì API dùng LPUSH).
    Trả về {"tenant_id", "job_id"} hoặc None khi hết thời gian chờ / phần tử hỏng.
    Dùng client stream vì lệnh chặn lâu (socket_timeout của client fast rất ngắn).
    """
    item = await get_redis_stream().brpop([JOB_QUEUE_KEY], timeout=timeout_seconds)
    if not item:
        return None

    _, raw = item
    try:
        data = json.loads(raw)
        tenant_id = str(data["tenant_id"])
        job_id = str(data["job_id"])
    except (ValueError, KeyError, TypeError):
        print(f"Bỏ qua phần tử hỏng trong {JOB_QUEUE_KEY}: {raw!r}", flush=True)
        return None

    return {"tenant_id": tenant_id, "job_id": job_id}
