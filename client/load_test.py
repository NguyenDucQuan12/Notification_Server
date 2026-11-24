"""
Kiểm thử tải END-TO-END cho toàn bộ hệ thống thông báo.

Khác với `noti_server/demo/noti_demo.py load` (chỉ thử Notification Server, tự XADD vào Redis),
công cụ này đi qua ĐÚNG luồng thật:

    POST /notifications/send (API)
        -> SQL Server: notifications + recipients + deliveries + outbox (1 transaction)
        -> publish theo lô vào Redis Stream (API ngay, hoặc Outbox Worker nếu quá DIRECT_PUBLISH_MAX_OUTBOX)
        -> Notification Server (Stream Hub) -> WebSocket -> N client

Cách làm:
1. Tạo N user giả lập (lt_u00001 ... ) và cấp ticket cho từng user thẳng vào Redis
   (giống việc API làm sau khi user đăng nhập; vì DB không có sẵn 2.000 tài khoản).
2. Mở N WebSocket tới Notification Server, channel "user".
3. Đăng nhập API bằng tài khoản có quyền notification:send (mặc định quan),
   gửi `--rounds` thông báo, mỗi thông báo tới CẢ N user.
4. Đo: thời gian API trả lời, số client nhận được, độ trễ từ lúc gọi API đến lúc client nhận.
5. Dọn dẹp: xóa dữ liệu thử trong SQL Server và stream của user giả lập trong Redis
   (thêm --keep-data nếu muốn giữ lại để xem).

YÊU CẦU: chạy cả API Server, Outbox Worker, Notification Server. Ví dụ:

    cd client
    python load_test.py --users 2000 --rounds 3

Mẹo: chạy load test trên máy khác với server sẽ cho số liệu độ trễ sát thực tế hơn.
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
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import redis.asyncio as redis_async
import websockets

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Stats:
    connected: int = 0
    connect_failed: int = 0
    disconnected: int = 0
    # notification_id -> danh sách thời điểm (perf_counter) các client nhận được
    received: dict[str, list[float]] = field(default_factory=dict)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    k = min(len(values) - 1, max(0, round(pct / 100 * (len(values) - 1))))
    return values[k]


async def create_ticket(r: redis_async.Redis, *, tenant: str, user_id: str) -> str:
    """Giống ws_ticket_service.create_ws_ticket của API (ticket dùng 1 lần, TTL ngắn)."""
    ticket = secrets.token_urlsafe(32)
    payload = {
        "tenant_id": tenant,
        "user_id": user_id,
        "username": user_id,
        "is_superuser": False,
        "permissions": ["notification:subscribe"],
        "channel": "user",
        "job_id": None,
    }
    await r.set(f"ws:ticket:{ticket}", json.dumps(payload), ex=120)
    return ticket


async def client_task(
    *,
    user_id: str,
    args: argparse.Namespace,
    r: redis_async.Redis,
    stats: Stats,
    connect_sem: asyncio.Semaphore,
    ready: asyncio.Event,
    stop: asyncio.Event,
) -> None:
    try:
        async with connect_sem:
            ticket = await create_ticket(r, tenant=args.tenant, user_id=user_id)
            # last_event_id = bây giờ -> chỉ nhận thông báo mới, không replay dữ liệu cũ.
            url = f"{args.ws}/ws/events?ticket={ticket}&last_event_id={int(time.time() * 1000)}-0"
            ws = await websockets.connect(url, open_timeout=30, ping_interval=None, max_queue=None)
    except Exception:  # noqa: BLE001
        stats.connect_failed += 1
        return

    stats.connected += 1
    try:
        async with ws:
            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                now = time.perf_counter()
                try:
                    event = json.loads(raw)
                except ValueError:
                    continue
                notification_id = event.get("notification_id")
                if notification_id:
                    stats.received.setdefault(notification_id, []).append(now)
    except websockets.ConnectionClosed:
        stats.disconnected += 1


async def login(client: httpx.AsyncClient, args: argparse.Namespace) -> str:
    resp = await client.post(f"{args.api}/auth/login", data={"username": args.username, "password": args.password})
    resp.raise_for_status()
    data = resp.json()
    token = (data.get("data") or {}).get("token") or {}
    return token.get("access_token") if isinstance(token, dict) else token or data.get("access_token")


async def run_personal_phase(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    user_ids: list[str],
    stats: Stats,
    args: argparse.Namespace,
    notification_ids: list[str],
) -> dict:
    """
    Gửi `--personal` thông báo, mỗi thông báo cho 1 user khác nhau, `--personal-concurrency`
    request cùng lúc (mô phỏng nhiều nghiệp vụ phát sinh thông báo riêng cho từng người).

    LƯU Ý: tất cả request đi từ 1 IP => phải thêm IP máy chạy test vào RATE_LIMIT_EXEMPT_IPS
    của API (env/.env.api), nếu không rate-limit sẽ chặn/BAN IP này.
    """
    sem = asyncio.Semaphore(args.personal_concurrency)
    api_times: list[float] = []
    sent: dict[str, float] = {}
    errors = 0

    async def send_one(i: int) -> None:
        nonlocal errors
        user = user_ids[i % len(user_ids)]
        async with sem:
            start = time.perf_counter()
            try:
                resp = await client.post(
                    f"{args.api}/notifications/send",
                    json={
                        "title": f"Cá nhân #{i}",
                        "body": "Thông báo riêng cho bạn",
                        "target_user_ids": [user],
                        "notification_type": "loadtest.personal",
                    },
                    headers=headers,
                )
            except Exception:  # noqa: BLE001
                errors += 1
                return
            api_times.append(time.perf_counter() - start)
            if resp.status_code != 200:
                errors += 1
                return
            nid = resp.json()["data"]["notification_id"]
            sent[nid] = start
            notification_ids.append(nid)

    print(f"Gửi {args.personal} thông báo cá nhân, {args.personal_concurrency} request đồng thời ...")
    t0 = time.perf_counter()
    await asyncio.gather(*[send_one(i) for i in range(args.personal)])
    seconds = time.perf_counter() - t0

    deadline = time.perf_counter() + args.timeout
    while time.perf_counter() < deadline:
        if all(stats.received.get(nid) for nid in sent):
            break
        await asyncio.sleep(0.1)

    latencies = [stats.received[nid][0] - st for nid, st in sent.items() if stats.received.get(nid)]
    return {
        "ok": len(sent), "errors": errors, "seconds": seconds,
        "api_p50": percentile(api_times, 50), "api_p95": percentile(api_times, 95),
        "api_p99": percentile(api_times, 99),
        "delivered": len(latencies),
        "lat_p50": percentile(latencies, 50), "lat_p95": percentile(latencies, 95),
        "lat_p99": percentile(latencies, 99),
    }


async def cleanup(notification_ids: list[str], user_ids: list[str], args: argparse.Namespace,
                  r: redis_async.Redis) -> None:
    """Xóa dữ liệu thử: Redis stream của user giả lập + các dòng SQL của thông báo thử."""
    keys = [f"stream:noti:tenant:{args.tenant}:user:{u}" for u in user_ids]
    for start in range(0, len(keys), 500):
        await r.delete(*keys[start:start + 500])

    if not notification_ids:
        return
    # Dùng chung cấu hình/kết nối DB của API (env/.env.api) - đây là công cụ dev.
    sys.path.insert(0, str(PROJECT_ROOT / "fastapi_server" / "src"))
    from sqlalchemy import text  # noqa: PLC0415
    from db.database import AsyncSessionLocal, engine  # noqa: PLC0415

    ids_json = json.dumps(notification_ids)
    async with AsyncSessionLocal() as db:
        async with db.begin():
            for table, column in (
                ("notification_outbox", "aggregate_id"),
                ("notification_deliveries", "notification_id"),
                ("notification_recipients", "notification_id"),
                ("notifications", "notification_id"),
            ):
                await db.execute(
                    text(
                        f"DELETE FROM {table} WHERE {column} IN "
                        "(SELECT j.id FROM OPENJSON(:ids) WITH (id varchar(64) '$') AS j)"
                    ),
                    {"ids": ids_json},
                )
    await engine.dispose()
    print(f"Đã xóa dữ liệu thử: {len(notification_ids)} thông báo, {len(keys)} stream Redis.")


async def main(args: argparse.Namespace) -> None:
    user_ids = [f"lt_u{i:05d}" for i in range(1, args.users + 1)]
    pool = redis_async.BlockingConnectionPool.from_url(
        args.redis, decode_responses=True, max_connections=50, timeout=30,
    )
    r = redis_async.Redis.from_pool(pool)
    stats = Stats()
    stop = asyncio.Event()
    ready = asyncio.Event()
    notification_ids: list[str] = []

    # ---------------- 1. Mở N kết nối ----------------
    print(f"Mở {args.users} WebSocket tới {args.ws} ...")
    t0 = time.perf_counter()
    connect_sem = asyncio.Semaphore(args.connect_concurrency)
    tasks = [
        asyncio.create_task(client_task(
            user_id=u, args=args, r=r, stats=stats,
            connect_sem=connect_sem, ready=ready, stop=stop,
        ))
        for u in user_ids
    ]
    while stats.connected + stats.connect_failed < args.users:
        await asyncio.sleep(0.2)
    connect_seconds = time.perf_counter() - t0
    print(f"Kết nối: {stats.connected}/{args.users} thành công trong {connect_seconds:.1f}s "
          f"(lỗi {stats.connect_failed})")
    # Chờ Stream Hub đưa các stream mới đăng ký vào vòng XREAD kế tiếp.
    await asyncio.sleep(2)

    rounds: list[dict] = []
    personal = None
    health = None
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            token = await login(client, args)
            headers = {"Authorization": f"Bearer {token}"}

            # ---------------- 2. Gửi thông báo ----------------
            for rnd in range(1, args.rounds + 1):
                body = {
                    "title": f"Load test #{rnd}",
                    "body": f"Thông báo kiểm thử tải gửi tới {args.users} người dùng",
                    "target_user_ids": user_ids,
                    "notification_type": "loadtest.message",
                    "mode": args.mode,
                }
                start = time.perf_counter()
                resp = await client.post(f"{args.api}/notifications/send", json=body, headers=headers)
                api_seconds = time.perf_counter() - start
                if resp.status_code != 200:
                    print(f"Round {rnd}: API lỗi {resp.status_code}: {resp.text[:300]}")
                    break
                data = resp.json()["data"]
                nid = data["notification_id"]
                notification_ids.append(nid)
                publish = data.get("publish") or {}
                how = "worker" if publish.get("deferred_to_worker") or args.mode == "worker" else "direct"

                # Chờ đến khi mọi client đang kết nối nhận được, hoặc hết thời gian.
                deadline = start + args.timeout
                while time.perf_counter() < deadline:
                    if len(stats.received.get(nid, [])) >= stats.connected:
                        break
                    await asyncio.sleep(0.05)

                times = [t - start for t in stats.received.get(nid, [])]
                rounds.append({
                    "round": rnd, "how": how, "api": api_seconds,
                    "got": len(times), "expected": stats.connected,
                    "p50": percentile(times, 50), "p95": percentile(times, 95),
                    "p99": percentile(times, 99), "max": max(times) if times else float("nan"),
                })
                print(f"Round {rnd}: API {api_seconds * 1000:.0f}ms ({how}), "
                      f"nhận {len(times)}/{stats.connected}, "
                      f"100% sau {rounds[-1]['max'] * 1000:.0f}ms")
                if rnd < args.rounds:
                    await asyncio.sleep(args.interval)

            # ---------------- 2b. Thông báo cá nhân gửi đồng thời ----------------
            personal = None
            if args.personal > 0:
                personal = await run_personal_phase(client, headers, user_ids, stats, args, notification_ids)

            try:
                health = (await client.get(f"{args.ws.replace('ws://', 'http://').rsplit('/noti', 1)[0]}/health/stats")).json()
            except Exception:  # noqa: BLE001
                health = None
    finally:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    # ---------------- 3. Báo cáo ----------------
    print("\n" + "=" * 72)
    print(f"Người dùng / kết nối   : {stats.connected}/{args.users} (lỗi {stats.connect_failed}, "
          f"bị ngắt {stats.disconnected})")
    print(f"Thời gian mở kết nối   : {connect_seconds:.1f}s")
    print(f"{'Round':<6}{'Publish':<9}{'API(ms)':>9}{'Nhận':>13}{'p50(ms)':>10}{'p95(ms)':>10}"
          f"{'p99(ms)':>10}{'max(ms)':>10}")
    for x in rounds:
        print(f"{x['round']:<6}{x['how']:<9}{x['api'] * 1000:>9.0f}{x['got']:>7}/{x['expected']:<5}"
              f"{x['p50'] * 1000:>10.0f}{x['p95'] * 1000:>10.0f}{x['p99'] * 1000:>10.0f}{x['max'] * 1000:>10.0f}")
    total_expected = sum(x["expected"] for x in rounds)
    total_got = sum(x["got"] for x in rounds)
    if total_expected:
        print(f"Tổng thông báo nhận    : {total_got}/{total_expected} ({total_got / total_expected * 100:.2f}%)")
    if personal:
        print(f"Thông báo cá nhân      : {personal['ok']}/{args.personal} request thành công "
              f"(lỗi {personal['errors']}) trong {personal['seconds']:.1f}s "
              f"=> {personal['ok'] / personal['seconds']:.0f} thông báo/giây qua API")
        print(f"   API (ms)            : p50={personal['api_p50'] * 1000:.0f} "
              f"p95={personal['api_p95'] * 1000:.0f} p99={personal['api_p99'] * 1000:.0f}")
        print(f"   Client nhận         : {personal['delivered']}/{personal['ok']}, độ trễ (ms) "
              f"p50={personal['lat_p50'] * 1000:.0f} p95={personal['lat_p95'] * 1000:.0f} "
              f"p99={personal['lat_p99'] * 1000:.0f}")
    if health:
        print(f"Notification Server    : {json.dumps(health, ensure_ascii=False)[:400]}")
    print("(độ trễ tính từ lúc gọi API /notifications/send đến lúc client nhận qua WebSocket)")
    print("=" * 72)

    if not args.keep_data:
        await cleanup(notification_ids, user_ids, args, r)
    await r.aclose()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kiểm thử tải end-to-end hệ thống thông báo.")
    p.add_argument("--users", type=int, default=2000, help="Số user giả lập = số WebSocket.")
    p.add_argument("--rounds", type=int, default=3, help="Số thông báo gửi (mỗi cái tới tất cả user).")
    p.add_argument("--interval", type=float, default=2.0, help="Giây nghỉ giữa các round.")
    p.add_argument("--mode", choices=["direct", "worker"], default="direct")
    p.add_argument("--timeout", type=float, default=60.0, help="Giây chờ tối đa cho mỗi round.")
    p.add_argument("--connect-concurrency", type=int, default=200, help="Số kết nối mở đồng thời.")
    p.add_argument("--api", default=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"))
    p.add_argument("--ws", default=os.getenv("NOTI_WS_BASE_URL", "ws://127.0.0.1:1234/noti"))
    p.add_argument("--redis", default=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    p.add_argument("--tenant", default="system", help="Phải trùng tenant của tài khoản gửi.")
    p.add_argument("--username", default=os.getenv("NOTI_USERNAME", "quan"))
    p.add_argument("--password", default=os.getenv("NOTI_PASSWORD", "123456789"))
    p.add_argument("--personal", type=int, default=0,
                   help="Số thông báo cá nhân gửi đồng thời sau các round (0 = bỏ qua).")
    p.add_argument("--personal-concurrency", type=int, default=50, help="Số request /send chạy cùng lúc.")
    p.add_argument("--keep-data", action="store_true", help="Không xóa dữ liệu thử sau khi chạy.")
    return p.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        pass
