"""
Job Worker: xử lý file người dùng upload ở nền và gửi thông báo tiến trình.

Chạy: python fastapi_server\\src\\workers\\job_worker.py

Luồng của 1 job:

    API  : lưu file -> tạo job_record (queued) -> LPUSH queue:jobs -> thông báo job.queued
    Worker:
      1. BRPOP queue:jobs lấy {"tenant_id", "job_id"}
      2. Claim nguyên tử trong SQL: queued -> processing, attempts + 1
         (nhiều worker cùng chạy cũng không xử lý trùng 1 job)
      3. Thông báo job.started
      4. process_file(): mỗi mốc tiến độ -> cập nhật job_record + thông báo job.progress
      5a. Thành công -> status=success, progress=100, result_object_key -> thông báo job.succeeded
      5b. Lỗi tạm thời và còn lượt thử -> về queued, chờ rồi đẩy lại hàng đợi -> thông báo job.retrying
      5c. Lỗi nghiệp vụ hoặc hết lượt thử -> status=failed -> thông báo job.failed

Mọi thông báo đều đi qua send_custom_notification() (services/job_notifier.py), tức là cùng
luồng SQL outbox -> Redis Stream -> Notification Server -> WebSocket như các thông báo khác.

Lưới an toàn: định kỳ quét job 'processing' / 'queued' bị bỏ quên (worker chết, mất phần tử
trong Redis) và đẩy lại vào hàng đợi.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

# Mở comment 3 dòng bên dưới mỗi khi test (Chạy trực tiếp hàm if __main__)
# import os,sys
# PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(PROJECT_DIR)

from db.database import AsyncSessionLocal, close_db, configure_db_executor
from db.query_job_record import (
    claim_job_for_processing,
    mark_job_failed,
    mark_job_success,
    recover_stale_jobs,
    requeue_job_for_retry,
    update_job_progress,
)
from services.job_notifier import notify_job
from utils.job_processor import JobProcessingError, process_file
from utils.job_queue import dequeue_job, enqueue_job
from utils.job_storage import resolve_object_path, result_object_key
from services.redis_client import close_redis_clients

logger = logging.getLogger("job.worker")

# ------------------------------------------------------------------
# Cấu hình
# ------------------------------------------------------------------
# Số job xử lý song song trong 1 tiến trình worker.
CONCURRENCY = max(1, int(os.getenv("JOB_WORKER_CONCURRENCY", "2")))
# Thời gian chờ tối đa của BRPOP (giây). Cũng là độ trễ tối đa khi dừng worker.
POLL_SECONDS = float(os.getenv("JOB_WORKER_POLL_SECONDS", "5"))
# Số lần thử tối đa cho 1 job (tính cả lần đầu).
MAX_ATTEMPTS = max(1, int(os.getenv("JOB_MAX_ATTEMPTS", "3")))
# Chờ bao lâu trước khi thử lại (giây), nhân theo số lần đã thử.
RETRY_DELAY_SECONDS = float(os.getenv("JOB_RETRY_DELAY_SECONDS", "5"))
# Job 'processing'/'queued' không cập nhật quá thời gian này coi như bị bỏ quên.
STALE_AFTER_SECONDS = int(os.getenv("JOB_STALE_AFTER_SECONDS", "600"))
# Bao lâu quét job bị bỏ quên 1 lần.
RECOVER_EVERY_SECONDS = float(os.getenv("JOB_RECOVER_EVERY_SECONDS", "60"))


class JobWorker:
    """Nhiều "làn" (lane) cùng BRPOP hàng đợi; mỗi làn xử lý tuần tự từng job."""

    def __init__(self) -> None:
        self._stop = asyncio.Event()
        # Giữ tham chiếu các task đẩy lại hàng đợi sau khi chờ (tránh bị GC giữa chừng).
        self._retry_tasks: set[asyncio.Task[None]] = set()

    def request_stop(self) -> None:
        if not self._stop.is_set():
            logger.info("Stop requested, finishing current jobs...")
            self._stop.set()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------
    # Xử lý 1 job
    # ------------------------------------------------------------------
    async def handle(self, tenant_id: str, job_id: str) -> None:
        async with AsyncSessionLocal() as db:
            job = await claim_job_for_processing(db, tenant_id=tenant_id, job_id=job_id)
        if job is None:
            # Job đã được worker khác nhận, đã xong, hoặc đã bị xóa -> bỏ qua.
            logger.info("Skip job (not queued): %s/%s", tenant_id, job_id)
            return

        logger.info("Processing job %s/%s attempt=%s", tenant_id, job_id, job["attempts"])
        await notify_job(job, "started")

        async def report(progress: int, message: str) -> None:
            async with AsyncSessionLocal() as db:
                res = await update_job_progress(
                    db, tenant_id=tenant_id, job_id=job_id, progress=progress, message=message,
                )
            if res["success"]:
                await notify_job(res["data"], "progress", progress=progress, message=message)
            else:
                logger.warning("Không cập nhật được tiến độ job %s: %s", job_id, res["message"])

        result_key = result_object_key(tenant_id, job_id)
        try:
            summary = await process_file(
                job=job,
                input_path=resolve_object_path(job["upload_object_key"]),
                output_path=resolve_object_path(result_key),
                report=report,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - mọi lỗi xử lý đều phải được ghi nhận
            await self._on_failure(job, exc)
            return

        message = f"Xử lý hoàn tất ({summary['size_bytes']} byte)"
        async with AsyncSessionLocal() as db:
            res = await mark_job_success(
                db, tenant_id=tenant_id, job_id=job_id, result_object_key=result_key, message=message,
            )
        if not res["success"]:
            # Ghi DB lỗi: job vẫn 'processing' -> bước khôi phục sẽ đưa lại hàng đợi sau.
            logger.error("Không đánh dấu success được job %s: %s", job_id, res["message"])
            return

        await notify_job(
            res["data"], "succeeded",
            message=f"File '{job['filename']}' đã xử lý xong. Bấm để tải kết quả.",
        )
        logger.info("Job done %s/%s", tenant_id, job_id)

    async def _on_failure(self, job: dict[str, Any], exc: Exception) -> None:
        tenant_id, job_id = job["tenant_id"], job["job_id"]
        error = f"{type(exc).__name__}: {exc}"[:500]
        attempts = int(job.get("attempts") or 1)
        retryable = not isinstance(exc, JobProcessingError) and attempts < MAX_ATTEMPTS

        if retryable:
            delay = RETRY_DELAY_SECONDS * attempts
            logger.warning("Job %s lỗi (lần %s/%s), thử lại sau %.0fs: %s",
                           job_id, attempts, MAX_ATTEMPTS, delay, error)
            async with AsyncSessionLocal() as db:
                updated = await requeue_job_for_retry(
                    db, tenant_id=tenant_id, job_id=job_id, error=error,
                    message=f"Lỗi lần {attempts}/{MAX_ATTEMPTS}, sẽ thử lại",
                )
            await notify_job(
                updated or job, "retrying", error=error,
                message=f"Xử lý lỗi (lần {attempts}/{MAX_ATTEMPTS}), thử lại sau {delay:.0f} giây.",
            )
            task = asyncio.create_task(self._enqueue_later(tenant_id, job_id, delay))
            self._retry_tasks.add(task)
            task.add_done_callback(self._retry_tasks.discard)
            return

        logger.error("Job %s thất bại sau %s lần: %s", job_id, attempts, error)
        async with AsyncSessionLocal() as db:
            res = await mark_job_failed(db, tenant_id=tenant_id, job_id=job_id, error=error)
        await notify_job(res["data"] if res["success"] else job, "failed", error=str(exc))

    async def _enqueue_later(self, tenant_id: str, job_id: str, delay: float) -> None:
        await self._sleep(delay)
        try:
            await enqueue_job(tenant_id=tenant_id, job_id=job_id)
        except Exception:  # noqa: BLE001
            # Không đẩy được (Redis lỗi): job vẫn 'queued' trong DB, bước khôi phục sẽ đẩy lại.
            logger.exception("Không đẩy lại được job %s vào hàng đợi", job_id)

    # ------------------------------------------------------------------
    # Vòng lặp
    # ------------------------------------------------------------------
    async def _lane(self, lane_no: int) -> None:
        error_backoff = 1.0
        while not self._stop.is_set():
            try:
                item = await dequeue_job(POLL_SECONDS)
                if item:
                    await self.handle(item["tenant_id"], item["job_id"])
                error_backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - worker không được chết vì 1 lỗi tạm thời
                logger.exception("Lane %s error, retry in %.0fs", lane_no, error_backoff)
                await self._sleep(error_backoff)
                error_backoff = min(error_backoff * 2, 30.0)

    async def _recover_loop(self) -> None:
        while not self._stop.is_set():
            try:
                async with AsyncSessionLocal() as db:
                    stale = await recover_stale_jobs(db, stale_after_seconds=STALE_AFTER_SECONDS)
                for item in stale:
                    await enqueue_job(tenant_id=item["tenant_id"], job_id=item["job_id"])
                if stale:
                    logger.warning("Re-queued %s stale job(s)", len(stale))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Recover stale jobs failed")
            await self._sleep(RECOVER_EVERY_SECONDS)

    async def run(self) -> None:
        logger.info(
            "Job worker started: concurrency=%s max_attempts=%s stale_after=%ss",
            CONCURRENCY, MAX_ATTEMPTS, STALE_AFTER_SECONDS,
        )
        await asyncio.gather(
            self._recover_loop(),
            *(self._lane(i) for i in range(CONCURRENCY)),
        )
        for task in list(self._retry_tasks):
            task.cancel()
        logger.info("Job worker stopped")


async def main() -> None:
    configure_db_executor()
    worker = JobWorker()
    loop = asyncio.get_running_loop()

    # Linux/Docker: SIGTERM/SIGINT -> dừng êm. Windows: dùng KeyboardInterrupt ở dưới.
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, worker.request_stop)
        except (NotImplementedError, RuntimeError):
            pass

    try:
        await worker.run()
    finally:
        await close_redis_clients()
        await close_db()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
