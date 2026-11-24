"""
Stream Hub — đọc Redis Stream DÙNG CHUNG cho mọi WebSocket trong process.

VẤN ĐỀ CỦA CÁCH CŨ (mỗi WebSocket tự XREAD BLOCK):
    5.000 client  =>  5.000 lệnh XREAD BLOCK  =>  5.000 kết nối Redis
    - Redis và pool kết nối cạn kiệt; mỗi event bị JSON-hóa lại cho từng client.

CÁCH MỚI ("một người trực tổng đài"):

        Redis Streams                    Stream Hub (1 process)                 WebSocket
    ┌──────────────────┐   XREAD 1 lệnh  ┌──────────────────────────────┐
    │ ...:user:u1      │◄───────────────►│ Shard 0: {user:u1, dash:t1…} │──┬──► queue ──► client A (u1, tab 1)
    │ ...:user:u2      │   cho NHIỀU     │                              │  └──► queue ──► client B (u1, tab 2)
    │ ...:dashboard    │   stream        │ Shard 1: {user:u2, …}        │─────► queue ──► client C (u2)
    │ stream:noti:glob │◄───────────────►│ Shard 2, 3 …                 │─────► queue ──► client D, E… (admin)
    └──────────────────┘                 └──────────────────────────────┘

- Mỗi shard là 1 task nền + 1 kết nối Redis, chạy vòng lặp XREAD BLOCK cho TẤT CẢ
  stream được giao (chia theo hash của stream key) => cả process chỉ dùng
  HUB_SHARDS kết nối Redis, dù có 10.000 WebSocket.
- Mỗi event được chuyển thành JSON đúng 1 LẦN rồi đẩy (chuỗi đã sẵn) vào hàng đợi
  của mọi subscriber đang nghe stream đó (vd 1 user mở 3 tab, 50 admin cùng xem dashboard).
- Stream không còn ai nghe sẽ tự bị bỏ khỏi lệnh XREAD.

ĐẢM BẢO KHÔNG MẤT / KHÔNG TRÙNG EVENT khi kết hợp với replay (xem noti_controller):
    1. subscribe()     : đăng ký TRƯỚC -> từ giờ mọi event mới đều vào hàng đợi.
    2. replay          : đọc lịch sử từ last_event_id tới cuối stream (XREVRANGE).
    3. đọc hàng đợi    : bỏ qua event có ID <= event cuối đã gửi (chống trùng),
                         gửi phần còn lại.
  Event nào xuất hiện trước bước 2 thì replay đã gửi; event nào xuất hiện sau bước 1
  thì hàng đợi có. Phần giao nhau được loại trùng bằng so sánh Stream ID.

KHI REDIS GẶP SỰ CỐ:
  Shard tự thử lại với backoff tăng dần (tối đa 5s), KHÔNG ngắt client. Cursor được
  giữ nguyên nên khi Redis trở lại, shard đọc tiếp từ đúng chỗ dừng => client
  nhận đủ event bị trễ. Trong lúc đó heartbeat báo stream_status="degraded".
"""
from __future__ import annotations

import asyncio
import logging
import zlib
from collections import Counter
from dataclasses import dataclass, field

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from config.configs import settings
from services.redis_client import get_redis_fast, get_redis_stream
from utils.events import fields_to_event
from utils.utils import STREAM_START_ID, safe_json_dumps

logger = logging.getLogger("noti.hub")


@dataclass(eq=False)  # eq=False: so sánh theo danh tính object, dùng được trong set().
class Subscription:
    """
    Đăng ký nghe 1 stream của 1 WebSocket.

    queue chứa các cặp (stream_id, json_text) đã sẵn sàng gửi thẳng cho client.
    """
    stream_key: str
    queue: asyncio.Queue[tuple[str, str]]
    # True khi hàng đợi đầy (client nhận quá chậm). Controller thấy cờ này sẽ ngắt kết nối.
    overflowed: bool = False

    def offer(self, stream_id: str, text: str) -> bool:
        """Đẩy event vào hàng đợi KHÔNG chờ. Trả False nếu hàng đợi đầy."""
        # Đã tràn trước đó → không nhận thêm (client sắp bị ngắt)
        if self.overflowed:
            return False
        try:
            self.queue.put_nowait((stream_id, text))
            return True
        except asyncio.QueueFull:
            self.overflowed = True
            return False

@dataclass
class _Shard:
    """
    1 shard = 1 task nền đọc 1 nhóm stream bằng 1 lệnh XREAD.
    - index: số thứ tự shard (0..HUB_SHARDS-1)
    - cursors: stream_key -> ID event cuối cùng shard đã đọc (cursor riêng của từng stream).
    - has_streams: được set khi shard có ít nhất 1 stream; shard rảnh sẽ ngủ chờ event này.
    - task: asyncio.Task chạy vòng lặp
    - healthy: True nếu shard đang đọc được Redis, False nếu shard bị lỗi Redis.
    """
    index: int
    # stream_key -> ID event cuối cùng shard đã đọc (cursor riêng của từng stream).
    cursors: dict[str, str] = field(default_factory=dict)
    # Được set khi shard có ít nhất 1 stream; shard rảnh sẽ ngủ chờ event này.
    has_streams: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None
    healthy: bool = True


class StreamHub:
    """
    Quản lý nhiều shard đọc Redis Stream DÙNG CHUNG cho mọi WebSocket trong process.
    - shards: danh sách shard (chia theo hash của stream key).
    - subscribers: stream_key -> tập Subscription đang nghe stream đó.
    - running: True nếu hub đang chạy, False nếu hub đã dừng.
    - stats: Counter thống kê số lượng event đọc/dispatch, số subscriber, số lỗi Redis, số event bị bỏ vì client chậm...
    """
    def __init__(self, *, shards: int) -> None:
        """
        Khởi tạo StreamHub. Gọi start() để chạy vòng lặp shard.
        """
        self._shards = [_Shard(index=i) for i in range(shards)]
        # stream_key -> tập subscriber đang nghe
        self._subscribers: dict[str, set[Subscription]] = {}
        self._running = False
        self._stats: Counter[str] = Counter()

    # ------------------------------------------------------------------
    # Vòng đời (gọi trong lifespan của main.py)
    # ------------------------------------------------------------------
    def start(self) -> None:
        """
        Bắt đầu vòng lặp của các shard. Gọi 1 lần duy nhất khi server khởi động.
        """
        if self._running:
            return
        self._running = True
        for shard in self._shards:
            shard.task = asyncio.create_task(self._run_shard(shard), name=f"hub-shard-{shard.index}")
        logger.info("Bắt đầu vòng lặp stream hub", extra={"shards": len(self._shards)})

    async def stop(self) -> None:
        """
        Dừng vòng lặp của các shard. Gọi khi server tắt.
        """
        self._running = False
        tasks = [s.task for s in self._shards if s.task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Stream hub đã dừng", extra={"shards": len(self._shards)})

    # ------------------------------------------------------------------
    # Đăng ký / hủy đăng ký
    # ------------------------------------------------------------------
    def _shard_for(self, stream_key: str) -> _Shard:
        """
        Tìm shard chịu trách nhiệm đọc stream_key.  
        Băm stream_key bằng CRC32 rồi chia lấy dư cho số shard → luôn ra CÙNG 1 shard cho CÙNG 1 stream.
        Dùng `crc32` thay vì `hash()` vì `hash()` của Python cho chuỗi bị ngẫu nhiên hóa mỗi lần chạy.
        Các stream được rải đều qua các shard.
        """
        return self._shards[zlib.crc32(stream_key.encode("utf-8")) % len(self._shards)]

    async def subscribe(self, stream_key: str) -> Subscription:
        """
        Đăng ký nghe stream. Gọi TRƯỚC khi replay

        Nếu đây là subscriber đầu tiên của stream, đặt cursor của shard = ID event
        mới nhất hiện có, để shard chỉ đọc event MỚI từ thời điểm này (phần cũ
        do replay của từng client tự lo).
        """
        # Tạo Subscription mới cho WebSocket, với hàng đợi asyncio.Queue có giới hạn (mặc định 1000 event)
        sub = Subscription(
            stream_key=stream_key,
            queue=asyncio.Queue(maxsize=settings.HUB_SUBSCRIBER_QUEUE_SIZE),
        )

        # Tìm shard chịu trách nhiệm đọc stream_key
        shard = self._shard_for(stream_key)
        # Nếu shard chưa có cursor cho stream này, đọc ID event mới nhất hiện có trong Redis để đặt cursor.
        start_id: str | None = None
        if stream_key not in shard.cursors:
            # Có await ở đây -> sau await phải kiểm tra lại (subscriber khác có thể đã thêm stream).
            latest = await get_redis_fast().xrevrange(stream_key, max="+", min="-", count=1)
            start_id = latest[0][0] if latest else STREAM_START_ID

        # Kiểm tra LẠI: trong lúc await ở trên, subscriber khác có thể đã thêm stream này
        if stream_key not in shard.cursors:
            shard.cursors[stream_key] = start_id or STREAM_START_ID
            # Nếu shard đang ngủ vì không có stream nào, set event để shard thức dậy và đọc stream mới.
            shard.has_streams.set()

        # Thêm subscriber vào tập của stream_key. Khi shard đọc được event mới, sẽ phát cho mọi subscriber trong tập này.
        self._subscribers.setdefault(stream_key, set()).add(sub)
        self._stats["subscribe_total"] += 1
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        """
        Hủy đăng ký. Stream không còn ai nghe thì bỏ khỏi XREAD của shard.
        """
        # Lấy tập subscriber của stream_key
        subs = self._subscribers.get(sub.stream_key)
        if not subs:
            return

        # Bỏ subscriber khỏi tập
        subs.discard(sub)
        if not subs:
            # Xóa stream_key khỏi shard nếu không còn ai nghe
            del self._subscribers[sub.stream_key]
            # Tìm shard chịu trách nhiệm đọc stream_key và xóa cursor của stream này
            shard = self._shard_for(sub.stream_key)
            shard.cursors.pop(sub.stream_key, None)
            # Nếu shard không còn stream nào, xóa event để shard ngủ chờ stream mới.
            if not shard.cursors:
                shard.has_streams.clear()

    # ------------------------------------------------------------------
    # Vòng lặp của 1 shard
    # ------------------------------------------------------------------
    async def _run_shard(self, shard: _Shard) -> None:
        """
        Vòng lặp của 1 shard: đọc Redis Stream bằng 1 lệnh XREAD BLOCK cho TẤT CẢ stream shard này chịu trách nhiệm, phát event cho mọi subscriber.  
        Nếu Redis gặp sự cố, giữ nguyên cursor, thử lại với backoff tăng dần (tối đa 5s) → client vẫn nhận đủ event bị trễ. Trong lúc đó heartbeat báo stream_status="degraded".
        """
        redis_stream = get_redis_stream()
        backoff = 0.5

        while self._running:
            # Nếu không có stream nào cần đọc, shard ngủ chờ event has_streams được set.
            if not shard.cursors:
                # Không có stream nào cần nghe -> ngủ, không gửi lệnh vô ích tới Redis.
                await shard.has_streams.wait()
                continue

            # Chụp lại danh sách tại thời điểm gửi lệnh (dict có thể đổi trong lúc chờ).
            streams = dict(shard.cursors)
            try:
                response = await redis_stream.xread(
                    streams,                           # {stream_key: last_id, ...}: đọc event có ID > last_id ở TẤT CẢ stream trong 1 lệnh
                    block=settings.HUB_BLOCK_MS,       # Không có event thì Redis giữ lệnh tối đa 1000ms rồi trả rỗng
                    count=settings.STREAM_READ_COUNT,  # Mỗi stream lấy tối đa 100 event/lần
                )
            except (RedisConnectionError, RedisTimeoutError) as exc:
                # Mất kết nối Redis: giữ nguyên cursor, thử lại với backoff tăng dần.
                if shard.healthy:
                    logger.error("Shard stream mất kết nối tới Redis", extra={"shard": shard.index, "error": repr(exc)})
                shard.healthy = False
                self._stats["redis_errors"] += 1
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue
            except RedisError as exc:
                # Lỗi logic (vd 1 key bị ghi đè thành kiểu khác không phải stream -> WRONGTYPE).
                logger.error("Shard stream gặp lỗi khi đọc từ Redis", extra={"shard": shard.index, "error": repr(exc)})
                self._stats["redis_errors"] += 1
                # Bỏ stream không phải kiểu stream, shard sẽ bỏ nó khỏi XREAD ở vòng lặp tiếp theo.
                await self._drop_invalid_streams(shard, streams)
                await asyncio.sleep(0.5)
                continue
            except Exception:  # pylint: disable=broad-except
                # Lỗi bất ngờ, không biết lý do. Ghi log và thử lại
                logger.exception("Lỗi bất ngờ trong shard stream", extra={"shard": shard.index})
                await asyncio.sleep(1.0)
                continue

            if not shard.healthy:
                logger.info("Shard stream sẽ được kết nối lại", extra={"shard": shard.index})
            shard.healthy = True
            backoff = 0.5

            # Nếu có event mới, phát cho mọi subscriber. response = [[stream_key, [(stream_id, {field: value}), ...]], ...].
            if response:
                self._dispatch(shard, response)

    def _dispatch(self, shard: _Shard, response: list) -> None:
        """Phát event cho các subscriber. Đồng bộ (không await) nên rất nhanh."""
        # Lặp từng stream có event mới
        for stream_key, entries in response:
            if stream_key not in shard.cursors:
                logger.info("Stream này đã bị hủy đăng ký trong lúc chờ XREAD", extra={"stream_key": stream_key})
                continue

            # Lặp từng event mới của stream này, phát cho mọi subscriber đang nghe stream_key
            for stream_id, fields in entries:
                self._stats["events_read"] += 1
                subs = self._subscribers.get(stream_key)
                if subs:
                    # JSON-hóa 1 lần, dùng chung cho mọi subscriber của stream.
                    text = safe_json_dumps(fields_to_event(stream_id, fields))
                    for sub in tuple(subs):
                        # Đẩy dữ liệu vào hàng đợi
                        if sub.offer(stream_id, text):
                            self._stats["events_dispatched"] += 1
                        else:
                            self._stats["events_dropped_slow_client"] += 1

                # Cập nhật cursor của stream này trong shard
                shard.cursors[stream_key] = stream_id

    async def _drop_invalid_streams(self, shard: _Shard, streams: dict[str, str]) -> None:
        """
        Tìm key không phải kiểu stream và ngừng đọc nó (subscriber vẫn nhận heartbeat).
        Dùng khi shard gặp lỗi Redis (vd 1 key bị ghi đè thành kiểu khác không phải stream -> WRONGTYPE).
        """
        redis_fast = get_redis_fast()
        for key in streams:
            try:
                key_type = await redis_fast.type(key)
            except RedisError:
                return  # Redis có vấn đề chung, để vòng lặp chính xử lý.

            if key_type not in ("stream", "none"):
                logger.error("Key không phải là stream, bỏ qua", extra={"stream_key": key, "type": key_type})
                shard.cursors.pop(key, None)

    # ------------------------------------------------------------------
    # Trạng thái
    # ------------------------------------------------------------------
    @property
    def healthy(self) -> bool:
        """True nếu tất cả shard đều đọc được Redis, False nếu shard nào đó bị lỗi Redis."""
        return all(s.healthy for s in self._shards)

    def snapshot(self) -> dict:
        """
        Số liệu hiện tại, trả về qua /health/stream-hub.
        """
        return {
            "healthy": self.healthy,
            "shards": [
                {"index": s.index, "streams": len(s.cursors), "healthy": s.healthy}
                for s in self._shards
            ],
            "active_streams": len(self._subscribers),
            "subscriptions": sum(len(v) for v in self._subscribers.values()),
            "counters": dict(self._stats),
        }


# Instance dùng chung cho toàn bộ app (1 process = 1 hub).
stream_hub = StreamHub(shards=settings.HUB_SHARDS)
