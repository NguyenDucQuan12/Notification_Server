"""
Notification Outbox Worker — "lưới an toàn" đảm bảo KHÔNG notification nào bị mất.

Bối cảnh (Transactional Outbox Pattern):
1. API ghi notification + bản ghi notification_outbox trong CÙNG 1 transaction SQL.
2. Sau khi commit, API thử publish ngay vào Redis (Direct Publisher).
3. Nếu bước 2 lỗi (Redis chết, API crash giữa chừng...), outbox vẫn nằm trong DB
   với status='pending'. Worker này định kỳ quét và publish lại.

    API ──commit──> [notification_outbox: pending] ──(direct publish OK)──> published
                                 │
                                 └──(lỗi / API crash)──> Worker quét ──> published

Worker TÁI SỬ DỤNG đúng hàm publish_one_outbox_immediately() của API:
claim nguyên tử (pending -> processing) -> XADD Redis -> đánh dấu published/retry.
Vì vậy API và worker (kể cả chạy nhiều worker song song) không bao giờ publish trùng.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import time
from uuid import uuid4

# Mở comment 3 dòng bên dưới mỗi khi test (Chạy trực tiếp hàm if __main__)
import os,sys
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_DIR)

from db.database import AsyncSessionLocal, close_db, configure_db_executor
from db.query_notification_outbox import recover_stale_processing_outbox
from services.notification_publisher import publish_outbox_batch
from services.redis_client import close_redis_clients, get_redis_stream
from utils.redis_keys import OUTBOX_WAKEUP_KEY

logger = logging.getLogger("noti.outbox_worker")

# ------------------------------------------------------------------
# Cấu hình (đọc từ biến môi trường, có giá trị mặc định)
# ------------------------------------------------------------------
# Không có việc thì nghỉ bao lâu trước khi quét lại (giây).
POLL_SECONDS = float(os.getenv("NOTIFICATION_WORKER_POLL_SECONDS", "1"))
# Số outbox xử lý trong 1 lô (1 câu SQL claim + 1 Redis pipeline + 1 câu SQL cập nhật).
BATCH_SIZE = int(os.getenv("NOTIFICATION_WORKER_BATCH_SIZE", "500"))
# Số lô chạy song song KHI CÒN TỒN ĐỌNG (lô trước lấy đủ BATCH_SIZE). Bình thường chỉ chạy 1 lô.
# Các lô không giẫm chân nhau nhờ READPAST trong claim_outbox_batch.
# Muốn tăng thêm nữa: chạy nhiều tiến trình worker (an toàn, không publish trùng).
CONCURRENCY = max(1, int(os.getenv("NOTIFICATION_WORKER_CONCURRENCY", "4")))
# Outbox ở 'processing' quá thời gian này coi như bị kẹt (process xử lý đã chết).
STALE_AFTER_SECONDS = int(os.getenv("NOTIFICATION_WORKER_STALE_AFTER_SECONDS", "300"))
# Bao lâu chạy bước khôi phục outbox kẹt 1 lần (không cần chạy mỗi vòng).
RECOVER_EVERY_SECONDS = float(os.getenv("NOTIFICATION_WORKER_RECOVER_EVERY_SECONDS", "60"))


def build_worker_id() -> str:
    """ID duy nhất, ghi vào locked_by để biết worker nào đang xử lý outbox nào."""
    return f"worker:{socket.gethostname()}:{uuid4().hex[:8]}"


class OutboxWorker:
    """
    Worker quét outbox và publish vào Redis Stream.
    Worker này chạy song song với API, nhưng không publish trùng nhờ claim nguyên tử (pending -> processing) trước khi XADD Redis.
    """
    def __init__(self) -> None:
        self.worker_id = build_worker_id()
        # Event báo hiệu dừng: SIGTERM (docker stop) / Ctrl+C sẽ set event này.
        # Worker hoàn thành NỐT batch đang xử lý rồi mới thoát (graceful shutdown),
        # không bỏ outbox ở trạng thái processing giữa chừng.
        self._stop = asyncio.Event()
        self._last_recover = 0.0
        # Số lô chạy song song ở vòng kế tiếp (tăng lên CONCURRENCY khi có tồn đọng).
        self._lanes = 1

    def request_stop(self) -> None:
        if not self._stop.is_set():
            logger.info("Stop requested, finishing current batch...")
            self._stop.set()

    async def _sleep(self, seconds: float) -> None:
        """Ngủ nhưng thức dậy ngay khi có yêu cầu dừng."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _wait_for_work(self, seconds: float) -> None:
        """
        Chờ việc mới tối đa `seconds` giây.

        BLPOP trên OUTBOX_WAKEUP_KEY: API đẩy tín hiệu vào đây ngay sau khi ghi outbox
        => worker thức dậy gần như tức thì thay vì chờ hết chu kỳ poll.
        Redis lỗi -> quay về ngủ bình thường (vẫn quét theo chu kỳ, không mất outbox).
        """
        if self._stop.is_set():
            return
        try:
            woke = await get_redis_stream().blpop([OUTBOX_WAKEUP_KEY], timeout=seconds)
            if woke:
                # Gom các tín hiệu dồn lại: 1 lần quét xử lý được hết.
                await get_redis_stream().delete(OUTBOX_WAKEUP_KEY)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            await self._sleep(seconds)

    async def _recover_if_due(self) -> None:
        now = time.monotonic()
        if now - self._last_recover < RECOVER_EVERY_SECONDS:
            return
        self._last_recover = now

        async with AsyncSessionLocal() as db:
            async with db.begin():
                recovered = await recover_stale_processing_outbox(
                    db, stale_after_seconds=STALE_AFTER_SECONDS,
                )
        if recovered:
            logger.warning("Recovered stale outbox rows: %s", recovered)

    async def run_once(self) -> int:
        """
        1 vòng xử lý. Trả về số outbox đã thử publish (0 = không có việc).
        """
        await self._recover_if_due()

        lanes = self._lanes
        results = await asyncio.gather(
            *[
                publish_outbox_batch(worker_id=self.worker_id, limit=BATCH_SIZE)
                for _ in range(lanes)
            ],
            return_exceptions=True,
        )

        claimed = published = 0
        for res in results:
            if isinstance(res, BaseException):
                logger.error("Batch error: %s: %s", type(res).__name__, res)
                continue
            claimed += res["claimed_count"]
            published += res["published_count"]
            for failure in res["failed"]:
                logger.warning(
                    "Outbox publish failed: outbox_id=%s status=%s error=%s",
                    failure.get("outbox_id"), failure.get("outbox_status"),
                    failure.get("error_message"),
                )

        # Còn tồn đọng (mọi lô đều đầy) -> vòng sau chạy song song; hết tồn đọng -> về 1 lô.
        self._lanes = CONCURRENCY if claimed >= BATCH_SIZE * lanes else 1

        if claimed:
            logger.info(
                "Batch done: lanes=%s claimed=%s published=%s failed=%s",
                lanes, claimed, published, claimed - published,
            )
        return claimed

    async def run(self) -> None:
        logger.info(
            "Outbox worker started: id=%s batch=%s concurrency=%s poll=%ss",
            self.worker_id, BATCH_SIZE, CONCURRENCY, POLL_SECONDS,
        )
        error_backoff = 1.0

        while not self._stop.is_set():
            try:
                processed = await self.run_once()
                error_backoff = 1.0  # Thành công -> reset backoff.

                # Lấy đủ batch nghĩa là có thể còn việc -> làm tiếp ngay, không ngủ.
                if processed < BATCH_SIZE:
                    await self._wait_for_work(POLL_SECONDS)

            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - worker không được chết vì 1 lỗi tạm thời
                # Lỗi DB/mạng tạm thời: ghi log kèm traceback, chờ tăng dần (tối đa 30s) rồi thử lại.
                logger.exception("Worker loop error, retry in %.0fs", error_backoff)
                await self._sleep(error_backoff)
                error_backoff = min(error_backoff * 2, 30.0)

        logger.info("Outbox worker stopped")


async def main() -> None:
    # Thread pool đủ lớn cho aioodbc (xem giải thích trong db/database.py).
    configure_db_executor()
    worker = OutboxWorker()
    loop = asyncio.get_running_loop()

    # Linux/Docker: bắt SIGTERM (docker stop) và SIGINT (Ctrl+C) để dừng êm.
    # Windows không hỗ trợ add_signal_handler -> dùng KeyboardInterrupt ở dưới.
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, worker.request_stop)
        except (NotImplementedError, RuntimeError):
            pass

    try:
        await worker.run()
    finally:
        # Đóng kết nối Redis và DB pool khi thoát.
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
