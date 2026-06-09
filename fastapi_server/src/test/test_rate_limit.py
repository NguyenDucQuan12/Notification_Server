import time
import pytest
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from security.rate_limiter import rl_check, rl_key, SLIDING_WINDOW_SCRIPT, _redis

@pytest.fixture(autouse=True)
def ensure_redis():
    """
    Đảm bảo có kết nối Redis
    """
    pong = _redis.ping()
    assert pong is True

def _unique_ip():
    """
    Sinh IP giả (v4) để cách ly test (mỗi test một IP).
    """
    return f"127.0.0.{int(time.time()*1000) % 250}"

def test_allow_until_limit_then_deny():
    """
    Kiểm tra: với window_ms=1000, limit=3 → 3 request đầu allow, request thứ 4 deny.
    """
    ip = _unique_ip()
    bucket = "test1"
    window_ms = 1000
    limit = 3

    # Dọn key trước (nếu có)
    _redis.delete(rl_key(ip, bucket))
    _redis.delete(rl_key(ip, bucket) + ":seq")

    # Tiến hành chạy 4 vòng lặp giả gửi api
    results = []
    for i in range(4):
        allowed, count = rl_check(ip, bucket, window_ms, limit)
        print(f"Lần {i}:", allowed, count)
        results.append((allowed, count))

    # 3 cái đầu True, cái thứ 4 False
    assert results[0][0] is True and results[0][1] == 1
    assert results[1][0] is True and results[1][1] == 2
    assert results[2][0] is True and results[2][1] == 3
    assert results[3][0] is False and results[3][1] == 4  # đã vượt ngưỡng

def test_reset_after_window():
    """
    Sau khi đợi > window_ms, đếm trong cửa sổ phải 'mới' (bắt đầu lại từ 1).
    """
    ip = _unique_ip()
    bucket = "test2"
    window_ms = 500
    limit = 2

    _redis.delete(rl_key(ip, bucket))
    _redis.delete(rl_key(ip, bucket) + ":seq")

    # Gọi 2 lần ngay -> đủ limit
    assert rl_check(ip, bucket, window_ms, limit) == (True, 1)
    assert rl_check(ip, bucket, window_ms, limit) == (True, 2)
    # Lần 3 vượt
    allowed, count = rl_check(ip, bucket, window_ms, limit)
    assert allowed is False and count == 3

    # Đợi hết cửa sổ
    time.sleep((window_ms + 100) / 1000.0)

    # Gọi lại -> phải là lượt mới (1)
    allowed, count = rl_check(ip, bucket, window_ms, limit)
    assert allowed is True and count == 1

def test_member_uniqueness_under_burst():
    """
    Dồn nhiều request trong thời gian rất ngắn => nhờ member unique (now:seq) không rơi request.
    Kiểm tra count tăng đều từ 1..limit, rồi deny ở limit+1.
    """
    ip = _unique_ip()
    bucket = "test3"
    window_ms = 1000
    limit = 10

    _redis.delete(rl_key(ip, bucket))
    _redis.delete(rl_key(ip, bucket) + ":seq")

    # Gọi rất nhanh trong vòng for (có thể cùng ms)
    for i in range(1, limit + 1):
        allowed, count = rl_check(ip, bucket, window_ms, limit)
        assert allowed is True
        assert count == i  # phải tăng đều 1..limit

    # Lần tiếp theo phải deny
    allowed, count = rl_check(ip, bucket, window_ms, limit)
    assert allowed is False and count == limit + 1

def test_ttl_is_set_and_positive():
    """
    Sau khi gọi, key phải có TTL > 0 (đã đặt PEXPIRE).
    """
    ip = _unique_ip()
    bucket = "test4"
    window_ms = 800
    limit = 5

    key = rl_key(ip, bucket)
    seq_key = key + ":seq"
    _redis.delete(key)
    _redis.delete(seq_key)

    allowed, count = rl_check(ip, bucket, window_ms, limit)
    assert allowed is True and count == 1

    ttl = _redis.pttl(key)  # TTL ms
    seq_ttl = _redis.pttl(seq_key)
    # pttl có thể trả -1 (không TTL) hoặc -2 (không có key) nếu có trục trặc
    assert ttl is not None and ttl > 0
    assert seq_ttl is not None and seq_ttl > 0


# @pytest.mark.stress
def test_stress_burst_single_thread(caplog):
    """
    Burst đơn luồng: bắn N=1000 request thật nhanh vào cùng 1 IP/bucket.
    Kỳ vọng:
      - allowed đúng bằng limit
      - denied = N - limit
      - (tuỳ chọn) nếu đặt STRESS_THRESHOLD_MS, assert thời gian chạy <= ngưỡng
    Lưu ý: để tránh rơi request do trôi cửa sổ, dùng window_ms lớn (5000ms).
    """
    ip = _unique_ip()
    bucket = "stress_single"
    window_ms = 5000
    limit = 100
    N = 1000

    key = rl_key(ip, bucket)
    _redis.delete(key)
    _redis.delete(key + ":seq")

    allowed_cnt = 0
    denied_cnt = 0

    t0 = time.perf_counter()
    for _ in range(N):
        allowed, _ = rl_check(ip, bucket, window_ms, limit)
        if allowed:
            allowed_cnt += 1
        else:
            denied_cnt += 1
    elapsed_ms = (time.perf_counter() - t0) * 1000

    # Kiểm tra logic
    assert allowed_cnt == limit
    assert denied_cnt == N - limit

    # In/log để quan sát khi chạy -s
    print(f"[stress_single] elapsed={elapsed_ms:.2f} ms, allowed={allowed_cnt}, denied={denied_cnt}")

    # Tuỳ chọn: ép ngưỡng thời gian qua env (vd: STRESS_THRESHOLD_MS=200)
    thr = int(os.getenv("STRESS_THRESHOLD_MS", "0") or "0")
    if thr > 0:
        assert elapsed_ms <= thr, f"Burst quá chậm: {elapsed_ms:.2f}ms > {thr}ms"


# @pytest.mark.stress
def test_stress_concurrent_threads():
    """
    Burst đa luồng: 1000 request qua ThreadPoolExecutor (tối đa 50 luồng).
    Kỳ vọng:
      - allowed đúng bằng limit
      - denied = N - limit
    Dùng window_ms lớn để toàn bộ nằm trong cùng cửa sổ.
    """
    ip = _unique_ip()
    bucket = "stress_concurrent"
    window_ms = 5000
    limit = 200
    N = 1000
    max_workers = 50

    key = rl_key(ip, bucket)
    _redis.delete(key)
    _redis.delete(key + ":seq")

    def worker():
        allowed, _ = rl_check(ip, bucket, window_ms, limit)
        return allowed

    allowed_cnt = 0
    denied_cnt = 0

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker) for _ in range(N)]
        for fu in as_completed(futures):
            if fu.result():
                allowed_cnt += 1
            else:
                denied_cnt += 1
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"[stress_concurrent] elapsed={elapsed_ms:.2f} ms, allowed={allowed_cnt}, denied={denied_cnt}")

    assert allowed_cnt == limit
    assert denied_cnt == N - limit

    # Tuỳ chọn ngưỡng thời gian
    thr = int(os.getenv("STRESS_THRESHOLD_MS", "0") or "0")
    if thr > 0:
        assert elapsed_ms <= thr, f"Concurrent burst quá chậm: {elapsed_ms:.2f}ms > {thr}ms"


# Cách chạy test
# Tham số -s sẽ cho phép hiển thị các print trong tệp test (mặc định stdout bị chặn)
# python -m pytest -s -q src\test\test_rate_limit.py