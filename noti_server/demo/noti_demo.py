"""
Công cụ DEMO / KIỂM THỬ cho Notification Server.

!!! CHỈ DÙNG Ở MÔI TRƯỜNG DEV/DEMO !!!
Công cụ tự tạo ticket thẳng vào Redis (giả lập việc API server làm sau khi kiểm tra
JWT), và tự XADD event vào Redis Stream (giả lập API/worker publish). Nhờ vậy bạn
demo được Notification Server mà không cần chạy SQL Server + API.

Các lệnh (chạy từ thư mục noti_server):

    python demo/noti_demo.py ticket  --user u1                 # In ra URL WebSocket kèm ticket
    python demo/noti_demo.py listen  --user u1                 # Mở WebSocket và in event nhận được
    python demo/noti_demo.py publish --user u1 --count 5       # Gửi 5 thông báo cho u1
    python demo/noti_demo.py load    --connections 2000        # Kiểm thử tải: 2000 kết nối

Xem hướng dẫn đầy đủ: noti_server/DEMO.md
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as redis_async
import websockets
from websockets.exceptions import ConnectionClosed

# Dùng CHUNG hàm tạo key với server để chắc chắn khớp tên stream.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from utils.redis_keys import (  # noqa: E402
    global_stream_key,
    job_stream_key,
    tenant_stream_key,
    user_stream_key,
    ws_ticket_key,
)

# Console Windows mặc định không in được tiếng Việt -> ép UTF-8.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Quyền và đường dẫn tương ứng từng channel (khớp với api/noti_api.py).
CHANNELS: dict[str, dict[str, str]] = {
    "user": {"path": "/noti/ws/events", "permission": "notification:subscribe"},
    "tenant_dashboard": {"path": "/noti/ws/tenant-dashboard", "permission": "notification:tenant_dashboard"},
    "job": {"path": "/noti/ws/jobs/{job_id}", "permission": "notification:job_watch"},
    "global": {"path": "/noti/ws/global-events", "permission": "notification:global_subscribe"},
}


# ======================================================================
# Tiện ích chung
# ======================================================================

def redis_client(args: argparse.Namespace) -> redis_async.Redis:
    # BlockingConnectionPool: khi nhiều coroutine dùng Redis cùng lúc (lệnh load),
    # chúng xếp hàng chờ kết nối rảnh thay vì lỗi MaxConnectionsError (pool mặc định chỉ 100).
    pool = redis_async.BlockingConnectionPool.from_url(
        args.redis_url, decode_responses=True, max_connections=50, timeout=30,
    )
    return redis_async.Redis.from_pool(pool)  # from_pool: client sở hữu pool, aclose() đóng luôn pool.


async def create_ticket(
    r: redis_async.Redis,
    *,
    tenant: str,
    user: str,
    channel: str,
    job_id: str | None = None,
    permissions: list[str] | None = None,
    ttl: int = 60,
) -> str:
    """Giả lập API server: tạo ticket 1 lần, sống `ttl` giây (giống ws_ticket_service của API)."""
    ticket = secrets.token_urlsafe(32)
    payload = {
        "tenant_id": tenant,
        "user_id": user,
        "username": user,
        "permissions": permissions if permissions is not None else [CHANNELS[channel]["permission"]],
        "channel": channel,
        "job_id": job_id,
    }
    await r.set(ws_ticket_key(ticket), json.dumps(payload, ensure_ascii=False), ex=ttl)
    return ticket


def ws_url(args: argparse.Namespace, *, channel: str, ticket: str, job_id: str | None,
           last_event_id: str | None) -> str:
    path = CHANNELS[channel]["path"].format(job_id=job_id or "")
    url = f"{args.ws_base}{path}?ticket={ticket}"
    if last_event_id:
        url += f"&last_event_id={last_event_id}"
    return url


def stream_key_for(scope: str, *, tenant: str, user: str | None, job_id: str | None) -> str:
    if scope == "user":
        return user_stream_key(tenant, user or "")
    if scope == "tenant":
        return tenant_stream_key(tenant)
    if scope == "job":
        return job_stream_key(tenant, job_id or "")
    return global_stream_key()


# ======================================================================
# Lệnh: ticket
# ======================================================================

async def cmd_ticket(args: argparse.Namespace) -> None:
    r = redis_client(args)
    perms = args.permission  # None = quyền mặc định của channel; [] = không có quyền nào
    ticket = await create_ticket(r, tenant=args.tenant, user=args.user, channel=args.channel,
                                 job_id=args.job, permissions=perms, ttl=args.ttl)
    await r.aclose()
    print(f"Ticket (dùng 1 lần, hết hạn sau {args.ttl}s):\n  {ticket}\n")
    print("URL WebSocket:")
    print("  " + ws_url(args, channel=args.channel, ticket=ticket, job_id=args.job,
                        last_event_id=args.last_event_id))


# ======================================================================
# Lệnh: listen
# ======================================================================

def _short(event: dict[str, Any]) -> str:
    t = event.get("type")
    if t == "connected":
        return (f"KẾT NỐI THÀNH CÔNG  conn_id={event.get('connection_id')}  "
                f"last_event_id={event.get('last_event_id')}")
    if t == "heartbeat":
        return f"heartbeat  (stream_status={event.get('stream_status')})"
    if t == "replay_incomplete":
        return f"CẢNH BÁO: có thể đã mất event (reason={event.get('reason')}) -> nên đồng bộ qua REST API"
    if t == "pong":
        return "pong"
    if t == "server_error":
        return f"LỖI SERVER: {event.get('message')}"
    return (f"EVENT {event.get('event_id')}  type={t}  "
            f"title={event.get('title')!r}  message={event.get('message')!r}")


async def cmd_listen(args: argparse.Namespace) -> None:
    """
    Client mẫu đầy đủ: xin ticket -> kết nối -> lưu last_event_id -> tự reconnect.

    last_event_id được lưu vào file để thử: Ctrl+C, publish thêm vài event, chạy lại
    listen -> server gửi bù đúng các event đã lỡ (replay).
    """
    r = redis_client(args)
    state_file = Path(f".demo_last_event_{args.channel}_{args.tenant}_{args.user}.txt")
    last_event_id = args.last_event_id or (state_file.read_text().strip() if state_file.exists() else None)
    if args.fresh:
        last_event_id = None

    print(f"Nghe channel={args.channel} tenant={args.tenant} user={args.user} "
          f"(last_event_id={last_event_id or 'không có'}). Ctrl+C để thoát.\n")

    while True:
        ticket = await create_ticket(r, tenant=args.tenant, user=args.user, channel=args.channel,
                                     job_id=args.job)
        url = ws_url(args, channel=args.channel, ticket=ticket, job_id=args.job, last_event_id=last_event_id)
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for raw in ws:
                    event = json.loads(raw)
                    stamp = time.strftime("%H:%M:%S")
                    if event.get("type") == "heartbeat" and not args.show_heartbeat:
                        continue
                    print(f"[{stamp}] {_short(event)}")
                    if args.verbose and event.get("event_id"):
                        print(json.dumps(event, ensure_ascii=False, indent=2))
                    # Chỉ event nghiệp vụ mới có event_id; lưu lại để reconnect không mất event.
                    if event.get("event_id"):
                        last_event_id = event["event_id"]
                        state_file.write_text(last_event_id)
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd else None
            reason = exc.rcvd.reason if exc.rcvd else ""
            print(f"\nKết nối đóng: code={code} reason={reason}")
            if code in (4400, 4403):
                print("Không đủ quyền / request sai -> dừng.")
                break
            delay = 15 if code in (1013, 4429) else 1 if code == 1012 else 3
            print(f"Tự kết nối lại sau {delay}s...\n")
            await asyncio.sleep(delay)
        except OSError as exc:
            print(f"Không kết nối được server ({exc}). Thử lại sau 3s...")
            await asyncio.sleep(3)
    await r.aclose()


# ======================================================================
# Lệnh: publish
# ======================================================================

async def cmd_publish(args: argparse.Namespace) -> None:
    """Giả lập API/worker: XADD event vào Redis Stream (giống notification_publisher)."""
    r = redis_client(args)
    key = stream_key_for(args.scope, tenant=args.tenant, user=args.user, job_id=args.job)
    for i in range(1, args.count + 1):
        fields = {
            "type": args.type,
            "event_id": f"evt_demo_{secrets.token_hex(4)}",   # ID nghiệp vụ -> client thấy ở source_event_id
            "tenant_id": args.tenant,
            "title": f"{args.title} #{i}" if args.count > 1 else args.title,
            "message": args.message,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "metadata": json.dumps({"index": i, "source": "noti_demo"}, ensure_ascii=False),
        }
        stream_id = await r.xadd(key, fields, maxlen=10000, approximate=True)
        print(f"Đã gửi #{i} -> {key}  stream_id={stream_id}")
        if i < args.count and args.interval > 0:
            await asyncio.sleep(args.interval)
    await r.aclose()


# ======================================================================
# Lệnh: load (kiểm thử tải)
# ======================================================================

class _LoadStats:
    def __init__(self) -> None:
        self.connected = 0
        self.failed: dict[str, int] = {}
        self.received = 0
        self.latencies_ms: list[float] = []


async def _load_client(args, r, stats: _LoadStats, idx: int, ready: asyncio.Event,
                       stop: asyncio.Event, sem: asyncio.Semaphore) -> None:
    user = f"load-u{idx % args.users}"
    async with sem:  # Giới hạn số kết nối đang bắt tay cùng lúc (tránh dồn dập).
        try:
            ticket = await create_ticket(r, tenant=args.tenant, user=user, channel="user")
            ws = await websockets.connect(
                ws_url(args, channel="user", ticket=ticket, job_id=None, last_event_id=None),
                open_timeout=30, ping_interval=None, max_queue=None,
            )
            first = json.loads(await asyncio.wait_for(ws.recv(), 30))
            if first.get("type") != "connected":
                raise RuntimeError(f"unexpected {first.get('type')}")
        except ConnectionClosed as exc:
            key = f"close_{exc.rcvd.code if exc.rcvd else 'none'}"
            stats.failed[key] = stats.failed.get(key, 0) + 1
            return
        except Exception as exc:  # noqa: BLE001
            key = type(exc).__name__
            stats.failed[key] = stats.failed.get(key, 0) + 1
            return
    stats.connected += 1
    if stats.connected == args.connections:
        ready.set()

    try:
        # "async for" nhẹ hơn nhiều so với wait_for(recv()) mỗi message (không tạo task phụ).
        # Task bị cancel() từ bên ngoài khi kết thúc đo.
        async for raw in ws:
            # Đo thời điểm nhận TRƯỚC khi parse JSON để không cộng thời gian xử lý của chính công cụ.
            received_at = time.time()
            if '"sent_at"' not in raw:
                continue  # connected / heartbeat
            event = json.loads(raw)
            stats.received += 1
            stats.latencies_ms.append((received_at - float(event["sent_at"])) * 1000)
    except (ConnectionClosed, asyncio.CancelledError):
        pass
    finally:
        await ws.close()


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p))]


async def cmd_load(args: argparse.Namespace) -> None:
    """
    Mở N kết nối (chia cho U user), gửi E đợt event, đo:
    - số kết nối thành công, số event nhận được / kỳ vọng
    - độ trễ từ lúc XADD tới lúc client nhận (p50/p95/p99/max)
    - số kết nối Redis server đang dùng cho XREAD (= HUB_SHARDS, không phụ thuộc N)
    """
    args.users = min(args.users or max(1, args.connections // 2), args.connections)
    # Mỗi lần chạy dùng tenant riêng => stream mới tinh. Nếu dùng lại stream cũ, kết nối mới
    # sẽ được REPLAY event của lần chạy trước (đúng thiết kế) làm sai số liệu đo.
    args.tenant = f"{args.tenant}-{secrets.token_hex(3)}"
    r = redis_client(args)
    stats = _LoadStats()
    ready, stop = asyncio.Event(), asyncio.Event()
    sem = asyncio.Semaphore(args.connect_concurrency)

    print(f"Mở {args.connections} kết nối cho {args.users} user (tenant={args.tenant})...")
    t0 = time.perf_counter()
    tasks = [asyncio.create_task(_load_client(args, r, stats, i, ready, stop, sem))
             for i in range(args.connections)]

    # Chờ tất cả kết nối xong (hoặc lỗi), in tiến độ.
    while True:
        done = stats.connected + sum(stats.failed.values())
        print(f"\r  đã kết nối: {stats.connected}  lỗi: {sum(stats.failed.values())}", end="", flush=True)
        if done >= args.connections:
            break
        await asyncio.sleep(0.5)
    print(f"\n  -> mất {time.perf_counter() - t0:.1f}s. Lỗi chi tiết: {stats.failed or 'không có'}")
    if stats.connected == 0:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await r.aclose()
        return

    expected = args.events * stats.connected if not stats.failed else None

    print(f"\nGửi {args.events} đợt, mỗi đợt 1 event cho MỖI user ({args.users} event/đợt)...")
    for round_no in range(1, args.events + 1):
        pipe = r.pipeline(transaction=False)  # Gom lệnh gửi 1 lần cho nhanh.
        for u in range(args.users):
            pipe.xadd(user_stream_key(args.tenant, f"load-u{u}"),
                      {"type": "load.test", "round": str(round_no), "sent_at": repr(time.time())},
                      maxlen=1000, approximate=True)
        await pipe.execute()
        await asyncio.sleep(args.interval)

    # Chờ event cuối tới nơi.
    await asyncio.sleep(max(2.0, args.interval * 2))

    stats_json: dict[str, Any] = {}
    try:
        async with httpx.AsyncClient(timeout=5) as http:
            stats_json = (await http.get(f"{args.http_base}/health/stats")).json()
    except Exception as exc:  # noqa: BLE001
        print(f"(Không đọc được /health/stats: {exc})")

    stop.set()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await r.aclose()

    lat = stats.latencies_ms
    print("\n================ KẾT QUẢ ================")
    print(f"Kết nối thành công : {stats.connected}/{args.connections}")
    if expected is not None:
        print(f"Event nhận được    : {stats.received}/{expected} "
              f"({stats.received / expected * 100:.2f}%)")
    else:
        print(f"Event nhận được    : {stats.received} (có kết nối lỗi nên không tính tỉ lệ)")
    if lat:
        print(f"Độ trễ (ms)        : p50={_pct(lat, .50):.1f}  p95={_pct(lat, .95):.1f}  "
              f"p99={_pct(lat, .99):.1f}  max={max(lat):.1f}  avg={statistics.mean(lat):.1f}")
    hub = stats_json.get("stream_hub") or {}
    if hub:
        print(f"Server - kết nối WS đang mở : {stats_json.get('active_connections')}")
        print(f"Server - stream đang nghe   : {hub.get('active_streams')}")
        print(f"Server - kết nối Redis XREAD: {len(hub.get('shards', []))} (= HUB_SHARDS, KHÔNG tăng theo số client)")
        print(f"Server - bộ đếm hub         : {hub.get('counters')}")
    print("=========================================")


# ======================================================================
# CLI
# ======================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Công cụ demo Notification Server (chỉ dùng dev/demo)")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--ws-base", default=os.getenv("NOTI_WS_BASE", "ws://127.0.0.1:1234"))
    parser.add_argument("--http-base", default=os.getenv("NOTI_HTTP_BASE", "http://127.0.0.1:1234"))
    sub = parser.add_subparsers(dest="command", required=True)

    def identity(p: argparse.ArgumentParser) -> None:
        p.add_argument("--tenant", default="demo")
        p.add_argument("--user", default="u1")
        p.add_argument("--channel", default="user", choices=list(CHANNELS))
        p.add_argument("--job", default=None, help="job_id (bắt buộc khi --channel job)")

    p = sub.add_parser("ticket", help="Tạo ticket và in URL WebSocket")
    identity(p)
    p.add_argument("--permission", action="append", help="Ghi đè quyền (lặp lại được). Thử --permission none để xem lỗi 4403")
    p.add_argument("--ttl", type=int, default=60)
    p.add_argument("--last-event-id", default=None)
    p.set_defaults(func=cmd_ticket)

    p = sub.add_parser("listen", help="Mở WebSocket và in event nhận được")
    identity(p)
    p.add_argument("--last-event-id", default=None, help="Mặc định đọc từ file đã lưu lần trước")
    p.add_argument("--fresh", action="store_true", help="Bỏ qua last_event_id đã lưu")
    p.add_argument("--show-heartbeat", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true", help="In đầy đủ JSON của event")
    p.set_defaults(func=cmd_listen)

    p = sub.add_parser("publish", help="Gửi event vào Redis Stream (giả lập API/worker)")
    p.add_argument("--scope", default="user", choices=["user", "tenant", "job", "global"])
    p.add_argument("--tenant", default="demo")
    p.add_argument("--user", default="u1")
    p.add_argument("--job", default=None)
    p.add_argument("--type", default="notification.created")
    p.add_argument("--title", default="Có phiếu nhập kho mới")
    p.add_argument("--message", default="Phiếu PNK-001 vừa được tạo")
    p.add_argument("--count", type=int, default=1)
    p.add_argument("--interval", type=float, default=1.0, help="Giây giữa 2 event")
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("load", help="Kiểm thử tải nhiều kết nối")
    p.add_argument("--tenant", default="loadtest")
    p.add_argument("--connections", type=int, default=1000)
    p.add_argument("--users", type=int, default=0, help="Mặc định = connections/2 (mỗi user 2 tab)")
    p.add_argument("--events", type=int, default=10, help="Số đợt event")
    p.add_argument("--interval", type=float, default=0.5, help="Giây giữa 2 đợt")
    p.add_argument("--connect-concurrency", type=int, default=200)
    p.set_defaults(func=cmd_load)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if getattr(args, "channel", None) == "job" and not getattr(args, "job", None):
        sys.exit("--channel job cần --job <JOB_ID>")
    if getattr(args, "permission", None) == ["none"]:
        args.permission = []
    try:
        asyncio.run(args.func(args))
    except KeyboardInterrupt:
        print("\nĐã dừng.")


if __name__ == "__main__":
    main()
