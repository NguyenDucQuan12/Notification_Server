import time
import os
from dotenv import load_dotenv
from fastapi import Request
import asyncio
import threading
import ipaddress                         # Kiểm tra trusted proxies theo IP/CIDR
from dotenv import load_dotenv           # Nạp .env
from fastapi.responses import JSONResponse

from services.email_services import InternalEmailSender
from services.redis_client import get_redis_fast
from security.rate_limiter import rl_check
from security.config import TTL, RATE, BUCKET_BY_PATH, BAN_RULE, SUSPICIOUS_PATTERNS
from security.keyspace import (
    k_metric_req, k_metric_5xx, k_metric_bans,
    k_ban_ip, k_ban_notify, k_suspicious
)
from utils.get_ip_client import norm_ip, get_client_ip
from log.system_log import system_logger


load_dotenv()  # Tự động tìm và nạp file .env ở thư mục hiện tại

ALERT_REQ_PER_MIN  = int(os.getenv("ALERT_REQ_PER_MIN", 3000))  # ngưỡng req/min
ALERT_5XX_PER_MIN  = int(os.getenv("ALERT_5XX_PER_MIN", 100))   # ngưỡng 5xx/min
ALERT_BANS_PER_MIN = int(os.getenv("ALERT_BANS_PER_MIN", 30))   # ngưỡng bans/min


# Email admin nhận cảnh báo khi Redis outage kéo dài (chạm backoff 30s)
ADMIN_ALERT_EMAIL = os.getenv("EMAIL_ADMIN", "nguyenducquan2001@gmail.com").strip()

# Trusted proxies (danh sách IP hoặc CIDR, phân tách bằng dấu phẩy).
# Ví dụ: "127.0.0.1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
TRUSTED_PROXIES = [x.strip() for x in os.getenv("TRUSTED_PROXIES", "127.0.0.1").split(",") if x.strip()]

email_services = InternalEmailSender()                     # Service gửi email
redis_client = get_redis_fast()                            # Tạo Redis client dùng chung


# ============================================================
# CIRCUIT BREAKER CHO REDIS (FAIL-OPEN)
#    - Redis lỗi: cho request đi qua (không ban, không rate-limit, không cache…)
#    - Recheck sau 10s; mỗi lần fail +5s; max 30s
#    - Khi chạm 30s và vẫn lỗi => gửi mail admin (1 lần/đợt outage)
# ============================================================

# Lock để đồng bộ state giữa nhiều request đồng thời (tránh race-condition)
_REDIS_STATE_LOCK = asyncio.Lock()

# Redis down flag: True nghĩa là đang "cắt" Redis tạm thời (không dùng Redis)
_REDIS_DOWN = False

# Thời điểm (monotonic) được phép thử lại Redis (trước đó: skip toàn bộ Redis)
_REDIS_NEXT_CHECK_AT = 0.0

# Backoff hiện tại (giây): khởi đầu 10s, sau đó +5s mỗi lần fail, max 30s
_REDIS_BACKOFF_SECONDS = 10.0

# Đã gửi email cảnh báo outage cho "đợt down hiện tại" hay chưa
_REDIS_OUTAGE_NOTIFIED = False

async def _redis_ping() -> bool:
    """
    Ping Redis để kiểm tra sống/chết.
    - Dựa vào timeout trong redis_client.py nên nếu Redis chết sẽ không treo lâu.
    """
    try:
        # ping() trả True nếu Redis ok
        return bool(await redis_client.ping())
    except Exception:
        return False

async def _redis_available_now() -> bool:
    """
    Quyết định *ngay lúc này* có dùng Redis hay không (circuit breaker):
    - Nếu Redis đang OK => True
    - Nếu Redis đang DOWN:
        - Nếu chưa đến thời điểm recheck => False (skip Redis hoàn toàn)
        - Nếu đến thời điểm recheck => ping Redis:
            - Ping OK => reset state => True
            - Ping FAIL => tăng backoff (max 30), lên lịch check tiếp, nếu chạm 30 => gửi mail admin => False
    """
    global _REDIS_DOWN, _REDIS_NEXT_CHECK_AT, _REDIS_BACKOFF_SECONDS, _REDIS_OUTAGE_NOTIFIED

    now_mono = time.monotonic()  # Dùng monotonic để không bị ảnh hưởng khi system clock đổi

    # Đọc state dưới lock
    async with _REDIS_STATE_LOCK:
        down = _REDIS_DOWN
        next_check = _REDIS_NEXT_CHECK_AT

    # Nếu Redis đang không down => dùng bình thường
    if not down:
        return True

    # Nếu đang down nhưng CHƯA tới thời điểm check lại => skip Redis
    if now_mono < next_check:
        return False

    # Đã tới lúc check lại => ping Redis (ngoài lock để không giữ lock lâu)
    ok = await _redis_ping()
    
    # Đánh dấu có nên gửi mail hay không
    should_send_email = False

    # Cập nhật state dưới lock
    async with _REDIS_STATE_LOCK:
        if ok:
            # Redis đã hồi phục => reset hoàn toàn
            _REDIS_DOWN = False
            _REDIS_NEXT_CHECK_AT = 0.0
            _REDIS_BACKOFF_SECONDS = 10.0
            _REDIS_OUTAGE_NOTIFIED = False
            system_logger.info("Redis đã hồi phục, bật lại các chức năng Redis.")
            return True

        # Redis vẫn lỗi => giữ trạng thái down và tăng backoff
        _REDIS_DOWN = True
        _REDIS_BACKOFF_SECONDS = min(_REDIS_BACKOFF_SECONDS + 5.0, 30.0)  # +5s, max 30s
        _REDIS_NEXT_CHECK_AT = now_mono + _REDIS_BACKOFF_SECONDS          # Lịch check tiếp theo

        # Khi backoff chạm 30s và CHƯA gửi email trong đợt outage này => gửi email
        if _REDIS_BACKOFF_SECONDS >= 30.0 and not _REDIS_OUTAGE_NOTIFIED:
            _REDIS_OUTAGE_NOTIFIED = True  # Đánh dấu đã gửi để không spam mail
            should_send_email = True
    
    if should_send_email:
        email_services.send_mail_for_redis_crash(
                to_email=ADMIN_ALERT_EMAIL,
                reason="Redis không phản hồi ổn định. Middleware đang fail-open (cho request đi qua) và tạm tắt rate-limit/ban/metrics/cache."
            )

    return False

async def _redis_mark_down(desc: str, ex: Exception) -> None:
    """
    Đánh dấu Redis down ngay khi gặp exception Redis, và lên lịch retry theo backoff hiện tại.
    - Mục tiêu: request KHÔNG bị treo vì cố gọi Redis lặp lại.
    """
    global _REDIS_DOWN, _REDIS_NEXT_CHECK_AT, _REDIS_BACKOFF_SECONDS

    now_mono = time.monotonic()

    async with _REDIS_STATE_LOCK:
        # Nếu trước đó chưa down => khởi tạo down với backoff 10s
        if not _REDIS_DOWN:
            _REDIS_DOWN = True
            _REDIS_BACKOFF_SECONDS = 10.0  # reset về 10s cho đợt down mới

        # Lên lịch lần thử lại tiếp theo (sau backoff hiện tại)
        _REDIS_NEXT_CHECK_AT = now_mono + _REDIS_BACKOFF_SECONDS

    # Log warning để  biết Redis đang có vấn đề
    system_logger.warning("Redis không phản hồi (%s): %s. Circuit-breaker: Tạm bỏ qua Redis %ss.",
                          desc or "op", ex, _REDIS_BACKOFF_SECONDS)
    
async def _redis_safe(fn, *, default=None, desc: str = ""):
    """
    Wrapper gọi Redis an toàn:
    - Nếu Redis đang DOWN (circuit-breaker) => trả default ngay, không gọi Redis.
    - Nếu Redis đang OK => gọi fn().
        - Nếu fn() lỗi => đánh dấu Redis down + trả default.
    """
    # Nếu circuit-breaker đang cắt Redis => fail-open ngay
    if not await _redis_available_now():
        return default

    try:
        # Redis đang cho phép dùng => chạy thao tác Redis thực sự
        return await fn()
    except Exception as ex:
        # Có lỗi Redis => đánh dấu down để các request sau không bị treo
        await _redis_mark_down(desc=desc, ex=ex)
        return default

async def _is_banned(ip: str) -> bool:
    """
    Kiểm tra 1 IP có đang bị ban hay không với key định nghĩa trước  
    Nếu trong cache có key này và TTL vẫn đang tồn tại thì có nghĩa là đang bị ban
    """
    # TTL > 0 nghĩa là key tồn tại và còn hạn; TTL -1 không có TTL, -2 không có key
    ttl = await _redis_safe(lambda: redis_client.ttl(k_ban_ip(ip)), default=-2, desc="ttl(ban)")

    # TTL > 0 nghĩa là key còn hạn => đang ban
    if isinstance(ttl, int) and ttl > 0:
        return True

    # TTL == -1: có key nhưng không TTL (trường hợp defensive)
    if ttl == -1:
        return True

    # TTL == -2 hoặc 0 hoặc None => không ban
    return False

async def _ban_now(ip: str) -> None:
    """
    Đặt BAN ngay lập tức cho IP:
    - Ghi key `ban:ip:<ip>` với TTL = TTL.ban_seconds
    - Tăng metric bans theo phút để quan sát tần suất hệ thống đang chặn.
    """
    async def _op():
        # Ghi key ban với TTL value "1" (chỉ là flag) vào Redis
        await redis_client.setex(k_ban_ip(ip), TTL.ban_seconds, b"1")

        # Đếm bans/min để giám sát
        now_min = int(time.time() // 60)                           # bucket phút hiện tại
        pipe = redis_client.pipeline(transaction=False)            # pipeline giảm RTT
        pipe.incr(k_metric_bans(now_min))                          # tăng bans/min
        pipe.expire(k_metric_bans(now_min), TTL.metric_minute)     # TTL 3 phút cho counter phút (tự dọn sau TTL.metric_minute)
        await pipe.execute()                                             # thực thi pipeline

    await _redis_safe(_op, default=None, desc="ban_now")  # gọi an toàn

async def _mark_suspicious(ip: str) -> int:
    """
    Tăng đếm 'nghi vấn' cho IP trong 5 phút; trả về giá trị sau tăng.
    Nếu điểm nghi vấn vượt ngưỡng, sẽ do caller quyết định có BAN hay không.
    Mỗi lần tăng sẽ đặt TTL lại 5 phút (nên cứ tăng là giữ nguyên thời gian sống).
    """
    async def _op():
        key = k_suspicious(ip)                             # Tạo khoá điểm nghi vấn của IP trong cửa sổ 5 phút
        val = await redis_client.incr(key)                       # Tăng 1 giá trị (atomic trên Redis), nếu key này chưa có thì Redis tự tạo mới với giá trị 1
        if val == 1:
            await redis_client.expire(key, TTL.suspicious_5min)  # Nếu vừa tạo, đặt TTL 300s (5 phút)
        return int(val)  # trả score
    
    result = await _redis_safe(
        _op,
        default=0,
        desc="mark_suspicious",
    )

    return int(result or 0)

async def _notify_ban_once(ip: str, reason: str, path: str, ua: str) -> None:
    """
    Gửi email CHỈ LẦN ĐẦU khi IP bị BAN trong 1 chu kỳ BAN.  
    Dùng khoá 'ban:notify:<ip>' (SETNX) để chống spam email.
    Gửi email bằng background thread (không block request)
    """
    async def _op_setnx() -> bool:
        k_notify = k_ban_notify(ip)                                              # Tạo khoá chặn email gửi lặp lại
        ok = await redis_client.setnx(k_notify, b"1")                                  # True nếu chưa có -> lần đầu trong chu kỳ
        if ok:
            await redis_client.expire(k_notify, TTL.ban_seconds)                       # TTL trùng thời gian ban
        return bool(ok)  # trả kết quả

    first_time = await _redis_safe(_op_setnx, default=False, desc="ban_notify_setnx")  # setnx an toàn
    if not first_time:
        return                                                                   # không gửi nữa nếu đã gửi trong chu kỳ

    subject = f"[ALERT] BAN IP {ip}"                                             # tiêu đề email

    # Gửi email (Service Email đã luôn cấu hình gửi trong 1 luồng riêng )
    email_services.send_mail_alert(to_email="nguyenducquan2001@gmail.com",subject_mail=subject, 
                                        ip=ip, reason=reason, path_api=path, user_agent=ua, time_ban=TTL.ban_seconds)

def _is_ip_in_trusted_proxies(ip_str: str) -> bool:
    """
    Kiểm tra IP có thuộc nhóm trusted proxies không.
    - TRUSTED_PROXIES có thể chứa IP đơn lẻ (vd: 127.0.0.1)
      hoặc CIDR (vd: 10.0.0.0/8)
    """
    try:
        ip_obj = ipaddress.ip_address(ip_str)  # Parse IP client/proxy
    except Exception:
        return False  # IP không hợp lệ => không coi là trusted

    for item in TRUSTED_PROXIES:
        try:
            # Nếu item là CIDR (có '/'), parse thành network
            if "/" in item:
                net = ipaddress.ip_network(item, strict=False)
                if ip_obj in net:
                    return True
            else:
                # Nếu item là IP đơn lẻ, so sánh trực tiếp
                if ip_obj == ipaddress.ip_address(item):
                    return True
        except Exception:
            # Item cấu hình lỗi -> bỏ qua, không làm crash middleware
            continue

    return False

def _pick_bucket(path: str) -> str | None:
    """
    Match bucket theo prefix để không phụ thuộc exact path.
    - BUCKET_BY_PATH có thể chứa "/login" hoặc "/auth/login"
    - Nếu match -> trả bucket
    - Không match -> None
    """
    for p, b in BUCKET_BY_PATH.items():  # duyệt map
        if path == p or path.startswith(p.rstrip("/") + "/"):  # exact hoặc prefix
            return b  # bucket match
    return None  # không match

# ============================================================
# RATE LIMIT (LAZY IMPORT + FAIL-OPEN)
#    IMPORTANT:
#    - Tránh import rl_check ngay đầu file vì rate_limiter.py có thể "register_script" lúc import => Redis chưa chạy là sập app.
# ============================================================

_RL_CHECK_FUNC = None  # Cache function rl_check sau khi import thành công

async def _rl_check_safe(ip: str, bucket: str, *, window_ms: int, limit: int) -> bool:
    """
    Kiểm tra rate limit an toàn:
    - Nếu Redis DOWN => return True (fail-open)
    - Nếu import rate_limiter fail => return True (fail-open)
    - Nếu rl_check chạy lỗi => mark redis down + return True (fail-open)
    """
    global _RL_CHECK_FUNC

    # Circuit breaker đang cắt Redis => bỏ qua rate limit
    if not await _redis_available_now():
        return True

    # Lazy import để tránh crash lúc startup nếu Redis chưa chạy
    if _RL_CHECK_FUNC is None:
        try:
            from security.rate_limiter import rl_check  # import tại runtime
            _RL_CHECK_FUNC = rl_check                   # cache để dùng cho các request sau (không cần await vì ko phải chạy hàm)
        except Exception as ex:
            # Import fail (thường do rate_limiter.py connect Redis ngay lúc import)
            await _redis_mark_down(desc="import_rate_limiter", ex=ex)
            return True  # fail-open

    # Gọi rl_check trong _redis_safe để đảm bảo nếu Redis lỗi -> fail-open
    async def _op():
        allowed, _count = await _RL_CHECK_FUNC(ip, bucket, window_ms, limit)
        return bool(allowed)

    return bool(await _redis_safe(_op, default=True, desc=f"rl_check({bucket})"))



# ============================================================
#  MIDDLEWARE CHÍNH CHỊU TRÁCH NHIỆM KIỂM TRA CÁC REQUEST ĐI QUA
# ============================================================


async def security_guard(request: Request, call_next):
    """
    Middleware:
    - (A) Chuẩn hoá IP client
    - (B) Đếm req/min (metrics) (nếu Redis OK)
    - (C) Nếu IP đang bị BAN (nếu Redis OK) => 403 + email (1 lần/chu kỳ)
    - (D) Rate-limit global (nếu Redis OK): nếu vượt => tăng suspicious; nếu đạt ngưỡng => BAN
    - (E) Kiểm tra suspicious patterns (nếu Redis OK): tăng suspicious; nếu đạt ngưỡng => BAN
    - (F) Sau handler: nếu 5xx => đếm 5xx/min (nếu Redis OK)
    - Redis DOWN => FAIL-OPEN: cho request đi qua, không ban/rate-limit/metrics.
    """

    # Lấy IP client (trong Docker trực tiếp sẽ là IP client. Nếu sau reverse-proxy (Thường là sử dụng Nginx), nên đọc X-Forwarded-For và xác thực trusted proxies)
    client = get_client_ip(request= request)

    # Chuẩn hóa lại ip
    is_ip, client_ip = norm_ip(client)

    # Nếu địa chỉ IP không hợp lệ thì trả về lỗi
    if not is_ip:
        return JSONResponse(                                                            # Trả 400
            {"detail": "Không tìm thấy địa chỉ ip"},
            status_code=400
        )
    
    # ------------------------------------------------------------
    # NẾU REDIS ĐANG DOWN => FAIL-OPEN
    # (cho request đi qua, không làm gì liên quan Redis)
    # ------------------------------------------------------------
    if not await _redis_available_now():
        # Không gọi bất kỳ Redis nào để tránh treo
        return await call_next(request)

    # Đếm tổng request theo phút 
    now_min = int(time.time() // 60)                                                    # Lấy thời gian phút hiện tại

    # Đếm tổng req/min để quan sát, bằng cách an toàn với redis
    async def _op_req_metric():

        pipe = redis_client.pipeline(transaction=False)                                 # pipeline giảm RTT
        pipe.incr(k_metric_req(now_min))                                                # tăng req/min
        pipe.expire(k_metric_req(now_min), TTL.metric_minute)                           # TTL counter
        res = await pipe.execute()                                                            # chạy pipeline

        # res[0] là kết quả incr (count hiện tại)
        return int(res[0]) if res and res[0] is not None else 0                         # trả count
    
    req_count = int(await _redis_safe(_op_req_metric, default=0, desc="metric_req") or 0)     # Redis lỗi -> 0

    # Nếu req/min vượt ngưỡng -> hành động gì đó
    if req_count >= ALERT_REQ_PER_MIN:
        system_logger.warning(f"Requests/min vượt ngưỡng: {req_count} >= {ALERT_REQ_PER_MIN}. IP đang gọi API: {client_ip}")

    # Nếu IP đang bị BAN -> gửi email 1 lần/chu kỳ & trả 403 ngay (không vào handler)
    if await _is_banned(client_ip):
        await _notify_ban_once(
            ip=client_ip,
            reason="IP đang trong trạng thái bị chặn nhưng vẫn cố truy cập hệ thống",    # Lý do: IP ban vẫn cố truy cập
            path=str(request.url.path),
            ua=request.headers.get("user-agent", "")
        )

        return JSONResponse(                                # Trả 403 (Forbidden)
            {"detail": "Truy vấn của bạn hiện tại đang bị chặn vì tần suất yêu cầu lớn trong 1 thời gian nhất định."},
            status_code=403
        )

    # Rate-limit 'global' (sliding-window): nếu vượt limit -> tăng điểm nghi vấn; nếu đạt ngưỡng -> BAN + email + 403
    global_rate = RATE.get("global", {"window_ms": 60_000, "limit": 120})
    allowed = await _rl_check_safe(client_ip, "global", window_ms=global_rate["window_ms"], limit=global_rate["limit"])

    # Nếu vượt rate => tăng suspicious và có thể BAN
    if not allowed: 
        sus = await _mark_suspicious(client_ip)                                                 # Tăng điểm nghi vấn 5 phút
        if sus >= BAN_RULE["suspicious_per_5min"]:                                        # Nếu điểm ≥ ngưỡng -> BAN
            await _ban_now(client_ip)                                                           # đặt ban
            await _notify_ban_once(                                                       
                ip=client_ip,
                reason=f"Vượt rate-limit global trong 5 phút. suspicious={sus}",
                path=str(request.url.path),
                ua=request.headers.get("user-agent", "")
            )
            return JSONResponse(
                {"detail": "Bạn đã truy vấn liên tục trong thời gian ngắn, hệ thống sẽ giới hạn truy cập của bạn."},
                status_code=403
            )
        
        # CHƯA tới ngưỡng BAN -> CHO QUA, để handler vẫn hoạt động bình thường

    #  Rate-limit theo bucket nhạy cảm (ví dụ /login, /upload), các đường dẫn này cần được kiểm soát chặt hơn
    bucket = _pick_bucket(request.url.path)     # Tìm xem path thuộc bucket nào
    if bucket:
        # Lấy cấu hình window/limit cho bucket
        cfg = RATE.get(bucket)

        if not cfg:
            # Không có cấu hình bucket => không áp rate-limit
            system_logger.warning("Missing RATE config for bucket=%s; skip bucket rate-limit", bucket)
        else:
            try:
                ok2 = await _rl_check_safe(client_ip, bucket, window_ms=cfg["window_ms"], limit=cfg["limit"])  # check bucket
            except Exception as ex:
                system_logger.warning("rl_check(%s) failed: %s", bucket, ex)  # log
                ok2 = True  # fail-open

            if not ok2:
                sus = await _mark_suspicious(client_ip)                    # tăng suspicious
                if sus >= BAN_RULE["suspicious_per_5min"]:
                    await _ban_now(client_ip)
                    await _notify_ban_once(
                        ip=client_ip,
                        reason=f"Số lượt truy vấn vào đường dẫn bảo mật ({bucket}) trong 5 phút vượt ngưỡng cho phép: {sus}",
                        path=str(request.url.path),
                        ua=request.headers.get("user-agent", "")
                    )
                    return JSONResponse({"detail": "Bạn đã truy vấn liên tục vào hệ thống trong thời gian ngắn, hệ thống sẽ giới hạn truy cập của bạn."}, status_code=403)
                # CHƯA BAN -> CHO QUA

    # Gom chuỗi input từ query/path để check pattern đơn giản
    # Lưu ý: phần này chỉ chạy khi Redis OK (ở đầu đã check)
    query_string = str(request.url.query or "")
    path_string = str(request.url.path or "")
    payload_probe = (query_string + " " + path_string).lower()                            # chuẩn hoá URL

    # Nếu có pattern nghi vấn => cộng điểm
    if any(pat in payload_probe for pat in SUSPICIOUS_PATTERNS):                          # phát hiện pattern xấu
        sus = await _mark_suspicious(client_ip)                                                 # tăng điểm
        if sus >= BAN_RULE["suspicious_per_5min"]:                                        # đủ ngưỡng thì ban
            await _ban_now(client_ip)
            await _notify_ban_once(
                ip=client_ip,
                reason=f"Phát hiện mẫu nghi vấn (SQLi/XSS) và vượt ngưỡng 5 phút. suspicious={sus}",
                path=str(request.url.path),
                ua=request.headers.get("user-agent", "")
            )
            return JSONResponse(
                {"detail": "Hệ thống phát hiện truy vấn nghi vấn, bạn tạm thời bị chặn."},
                status_code=403
            )

    # UA rỗng/ngắn -> tăng điểm; nếu quá ngưỡng -> BAN
    ua = request.headers.get("user-agent", "")
    if not ua or len(ua) < 6:                                                               # UA quá ngắn thường là bot/thư viện quét
        sus = await _mark_suspicious(client_ip)
        if sus >= BAN_RULE["suspicious_per_5min"]:
            await _ban_now(client_ip)
            await _notify_ban_once(
                ip=client_ip,
                reason=f"Hệ thống nhận định bot hoặc thư viện gọi API (score={sus})",
                path=str(request.url.path),
                ua=ua
            )
            return JSONResponse({"detail": "Forbidden"}, status_code=403)

    # Không có lý do BAN -> Cho request đi qua hoàn toàn, không can thiệp header/status/body
    response = await call_next(request)

    # Sau handler: nếu server trả 5xx -> ghi nhận metric 5xx theo phút (để quan sát)
    if response.status_code >= 500:
        async def _op_5xx_metric():
            pipe = redis_client.pipeline(transaction=False)
            pipe.incr(k_metric_5xx(now_min))
            pipe.expire(k_metric_5xx(now_min), TTL.metric_minute)
            res = await pipe.execute()
            return int(res[0]) if res and res[0] is not None else 0

        s5xx = int(_redis_safe(_op_5xx_metric, default=0, desc="metric_5xx") or 0)

        if s5xx >= ALERT_5XX_PER_MIN:
            system_logger.warning("5xx/min vượt ngưỡng: %s >= %s", s5xx, ALERT_5XX_PER_MIN)

    # Trả response bình thường
    return response
