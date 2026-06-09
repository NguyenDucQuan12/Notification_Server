from typing import Any, List, Optional, TypeVar, Callable
from fastapi import HTTPException, status
from schemas.schemas import UserAuth
import os
import time
import threading
from log.system_log import system_logger
from fastapi_server.src.services.redis_client import get_redis                     
from security.config import TTL                       
from security.keyspace import (                                 
    k_ban_ip, k_ban_notify, k_suspicious
)
from utils.utils import _norm_ip
from services.email_services import InternalEmailSender
from utils.constants import *

# ------------ Lấy giá trị cấu hình từ biến môi trường ------------
REDIS_SCAN_COUNT  = int(os.getenv("REDIS_SCAN_COUNT", 2000))   # Số key/scan_iter vòng lặp
REDIS_BATCH_SIZE  = int(os.getenv("REDIS_BATCH_SIZE", 500))    # Số lệnh trong 1 batch pipeline
REDIS_RETRY       = int(os.getenv("REDIS_RETRY", 2))           # Số lần retry khi pipeline lỗi
REDIS_RETRY_SLEEP = float(os.getenv("REDIS_RETRY_SLEEP", 0.05))# Ngủ giữa hai lần retry (giây)

# Email admin để nhận cảnh báo Redis outage
SECURITY_ADMIN_EMAIL = os.getenv("EMAIL_ADMIN", "nguyenducquan2001@gmail.com")

# Tạo client Redis dùng chung
redis_client = get_redis()
email_services = InternalEmailSender()                     # Service gửi email


# =========================
# CIRCUIT BREAKER CHO REDIS
# =========================
# Mục tiêu:
# - Redis chết: không thử liên tục mỗi request (tránh treo/tốn tài nguyên)
# - Cho request đi qua (fail-open) với giá trị default
# - Thử lại Redis theo lịch backoff: 10s -> 15s -> 20s -> 25s -> 30s
# - Khi backoff chạm 30s: gửi mail cảnh báo (mỗi outage gửi 1 lần)


_T = TypeVar("_T")

# Lock để bảo vệ state khi chạy đa luồng (uvicorn/gunicorn có thể chạy nhiều worker/threads)
_redis_state_lock = threading.Lock()

# Trạng thái circuit breaker
_redis_down_until_ts: float = 0.0          # Nếu > now nghĩa là đang "mở circuit" (tạm bỏ Redis)
_redis_backoff_sec: int = 10               # Backoff hiện tại (10 -> 15 -> 20 -> 25 -> 30)
_redis_alert_sent: bool = False            # Đã gửi mail trong outage hiện tại chưa

def _redis_mark_down(reason: str) -> None:
    """
    Đánh dấu Redis đang DOWN và set lịch thử lại.
    Mỗi lần bị lỗi:
    - Nếu backoff < 30: tăng thêm 5
    - Nếu backoff đã 30: giữ 30
    - Khi backoff chạm 30 lần đầu trong outage -> gửi email cảnh báo
    """
    global _redis_down_until_ts, _redis_backoff_sec, _redis_alert_sent

    now = time.time()

    # Nếu đang trong outage và chưa tới lúc thử lại, không cần thay đổi nhiều
    # Tuy nhiên vẫn muốn đảm bảo down_until được set đúng.
    with _redis_state_lock:
        # Lịch thử lại = now + backoff hiện tại
        _redis_down_until_ts = now + float(_redis_backoff_sec)

        # Nếu backoff chưa đạt 30, tăng thêm 5 cho lần retry tiếp theo
        if _redis_backoff_sec < 30:
            _redis_backoff_sec = min(30, _redis_backoff_sec + 5)

        # Khi backoff đã đạt 30 và chưa gửi alert trong outage -> gửi 1 mail
        if _redis_backoff_sec >= 30 and not _redis_alert_sent:
            _redis_alert_sent = True  # đánh dấu trước để tránh spam khi mail gửi lỗi

            # Tiến hành gửi mail
            email_services.send_mail_for_redis_crash(to_email= SECURITY_ADMIN_EMAIL, reason= f"Redis đang lỗi/không truy cập được. Lý do gần nhất: {reason}")

def _redis_mark_up() -> None:
    """
    Đánh dấu Redis đã UP trở lại:
    - Reset backoff về 10
    - Reset alert flag (đợt outage mới sẽ gửi lại khi đạt 30)
    """
    global _redis_down_until_ts, _redis_backoff_sec, _redis_alert_sent

    with _redis_state_lock:
        _redis_down_until_ts = 0.0
        _redis_backoff_sec = 10
        _redis_alert_sent = False

def _redis_should_skip() -> bool:
    """
    Kiểm tra xem hiện tại có đang “bỏ Redis tạm thời” hay không.
    - True: đang skip Redis (fail-open)
    - False: được phép thử Redis
    """
    now = time.time()
    with _redis_state_lock:
        return now < _redis_down_until_ts
    
def _redis_health_check(desc: str) -> bool:
    """
    Thử ping Redis khi tới thời điểm retry.
    - Nếu ping OK: mark_up và trả True
    - Nếu ping lỗi: mark_down và trả False
    """
    # Nếu đang trong thời gian skip -> không ping nữa (tránh spam ping mỗi request)
    if _redis_should_skip():
        return False

    try:
        # ping nhẹ, timeout đã được cấu hình trong redis_client.py (socket_timeout)
        ok = bool(redis_client.ping())
        if ok:
            _redis_mark_up()
            return True
        else:
            _redis_mark_down(f"{desc}: ping returned False")
            return False
    except Exception as ex:
        _redis_mark_down(f"{desc}: ping exception: {ex}")
        return False

def _redis_safe(fn: Callable[[], _T], *, default: _T, desc: str) -> _T:
    """
    Wrapper gọi Redis an toàn:
    - Nếu Redis đang DOWN (circuit open) -> trả default ngay (fail-open)
    - Nếu tới lúc retry -> ping; nếu ping fail -> trả default
    - Nếu ping OK -> chạy fn(); nếu fn() lỗi -> mark_down và trả default
    """
    # Nếu đang skip Redis -> trả default ngay, không gọi Redis
    if _redis_should_skip():
        return default

    # Khi tới lượt thử lại Redis -> ping
    if not _redis_health_check(desc):
        return default

    # Redis có vẻ ổn -> thử chạy operation
    try:
        return fn()
    except Exception as ex:
        # Nếu operation lỗi -> đánh dấu Redis down, trả default
        _redis_mark_down(f"{desc}: op exception: {ex}")
        system_logger.warning("Redis operation failed (%s): %s", desc, ex)
        return default
    
def _pipeline_exec_safe(pipe, *, desc: str) -> Optional[List[Any]]:
    """
    Execute pipeline an toàn:
    - Có retry ngắn (REDIS_RETRY)
    - Nếu vẫn lỗi -> fail-open: trả None (caller tự xử lý)
    """
    def _op() -> List[Any]:
        # Retry ngắn cho các lỗi tạm (connection reset, timeout ngắn...)
        last_ex: Optional[Exception] = None
        for attempt in range(REDIS_RETRY + 1):
            try:
                return pipe.execute()
            except Exception as e:
                last_ex = e
                system_logger.warning(
                    "Redis pipeline failed (%s) attempt %s/%s: %s",
                    desc, attempt + 1, REDIS_RETRY + 1, e
                )
                if attempt < REDIS_RETRY:
                    time.sleep(REDIS_RETRY_SLEEP)
        # Hết retry mà vẫn lỗi -> raise để _redis_safe bắt và mark_down
        raise last_ex or RuntimeError("Redis pipeline failed without exception")

    # Nếu Redis lỗi -> default=None để caller biết “không có kết quả”
    return _redis_safe(_op, default=None, desc=desc)

# ===== Helper =====

def _ban_set(ip: str, ttl: Optional[int] = None) -> None:
    """
    Đặt cờ BAN cho IP với TTL (giây).
    Trả:
    - True  : ghi Redis thành công
    - False : Redis đang down hoặc ghi thất bại (fail-open)
    """
    def _op() -> bool:
        # TTL đầu vào; nếu None dùng mặc định
        t = int(ttl or TTL.ban_seconds)

        # TTL phải dương, nếu không dương thì coi như input invalid
        if t <= 0:
            # Lỗi input là lỗi logic, không liên quan Redis
            raise ValueError(f"Time To Live của Redis phải là số dương, giá trị nhận được: {t}")

        # Pipeline để gộp lệnh và giảm RTT
        with redis_client.pipeline(transaction=True) as p:
            p.setex(k_ban_ip(ip), t, b"1")                                        # key ban:ip:<ip> = "1" và TTL = t
            p.delete(k_ban_notify(ip))                                            # xoá notify để lần ban mới được gửi mail (nếu logic bạn cần)
            res = _pipeline_exec_safe(p, desc="ban_set_pipeline")

        # Nếu res là None => Redis down (fail-open)
        return res is not None

    # Redis down -> default=False (không set được)
    return _redis_safe(_op, default=False, desc="ban_set")

def _ban_ttl(ip: str) -> int:
    """
    Lấy TTL còn lại (giây) của IP đang BAN.
    - Trả về -2 nếu không có key; -1 nếu có key nhưng không TTL (không kỳ vọng vì ta luôn set TTL).
    - None nếu Redis down (fail-open)
    """
    return _redis_safe(
        lambda: int(redis_client.ttl(k_ban_ip(ip))),
        default=None,
        desc="ban_ttl"
    )

def _unban(ip: str) -> int:
    """
    Gỡ BAN (xoá key ban:ip:<ip>).
    - Redis trả 1 nếu xoá được, 0 nếu không tồn tại.
    - None nếu Redis down (fail-open)
    """
    return _redis_safe(
        lambda: int(redis_client.delete(k_ban_ip(ip))),
        default=None,
        desc="unban"
    )

class Security_Admin_Controller:
    """
    Controller để xử lý các vấn đê liên quan đến quản trị bảo mật (Security Admin)
    Yêu cầu:
    - user_info["Privilege"] ∈ {Admin, Boss} (HIGH_PRIVILEGE_LIST)
    """

    def ban_now(user_info: UserAuth, ip: str, ttl: Optional[int] = None) -> None:
        """
        Đặt BAN ngay 1 IP:
        - ip: chuỗi IPv4/IPv6
        - ttl: số giây; nếu None dùng TTL.ban_seconds
         BAN ngay một IP.

        Nếu Redis OK:
        - Ghi BAN vào Redis và trả applied=True

        Nếu Redis DOWN:
        - Không ghi được -> hệ thống vẫn chạy bình thường, trả applied=False + redis_ok=False
        """
        # Chuẩn hoá + validate IP
        is_ip, norm_ip = _norm_ip(ip_raw = ip)

        if (not is_ip):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "Message": f"Địa chỉ IP không hợp lệ: {ip}",
                })
        
        # Kiểm tra quyền hạn (bắt buộc phải là Admin/Boss)
        if not (user_info["Privilege"] in HIGH_PRIVILEGE_LIST):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "Message": f"Bạn không có quyền thực hiện thao tác này",
                }
            )
        
        # Thực hiện ban (fail-open)
        applied = _ban_set(norm_ip, ttl)

        # Trả kết quả rõ ràng cho phía client
        return {
            "IP": ip,
            "ttl_seconds": int(ttl or TTL.ban_seconds),
            "status": "banned" if applied else "redis_down_skip",
            "applied": bool(applied),                             # True nếu ghi được Redis
            "redis_ok": bool(applied),                            # ở đây applied ~ redis_ok cho thao tác này
        }
    
    def unban(user_info: UserAuth, ip: str) -> int:
        """
        Gỡ ban 1 IP:
        Redis OK:
        - deleted = 1/0 theo kết quả delete, 1 nếu xoá được key ban:ip:<ip>, 0 nếu không tồn tại

        Redis DOWN:
        - deleted=None, applied=False (không làm gì nhưng hệ thống không crash)
        """
        # Kiểm tra định dạng IP đơn giản
        is_ip, norm_ip = _norm_ip(ip_raw = ip)

        if (not is_ip):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "Message": f"Địa chỉ IP không hợp lệ: {ip}",
                })
        
        # Kiểm tra quyền hạn (bắt buộc phải là Admin/Boss)
        if not (user_info["Privilege"] in HIGH_PRIVILEGE_LIST):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "Message": f"Bạn không có quyền thực hiện thao tác này",
                }
            )
        
        deleted = _unban(norm_ip)
        # Redis down -> deleted=None
        if deleted is None:
            return {
                "ip": ip,
                "deleted": 0,
                "status": "redis_down_skip",
                "applied": False,
                "redis_ok": False
            }

        return {
            "ip": ip,
            "deleted": int(deleted),
            "status": "ok",
            "applied": True,
            "redis_ok": True
        }

    def unban_list(user_info: UserAuth, ips: List[str]):
        """
        Gỡ BAN cho nhiều IP.
        - Bỏ qua IP không hợp lệ (ghi 'error': 'invalid_ip' trong details)
        - Dùng pipeline theo lô + retry để tối ưu
        - Trả {"done": n, "total": m, "details":[...]}
        """
        # Kiểm tra quyền hạn (bắt buộc phải là Admin/Boss)
        if not (user_info["Privilege"] in HIGH_PRIVILEGE_LIST):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "Message": f"Bạn không có quyền thực hiện thao tác này",
                }
            )
        
        details: List[dict] = []
        done = 0

        # Gom IP hợp lệ vào danh sách để pipeline theo lô
        valid_ips: List[str] = []

        # Validate từng IP
        for ip in ips:
            # Kiểm tra định dạng IP đơn giản
            is_ip, norm_ip = _norm_ip(ip_raw = ip)

            if is_ip:
                valid_ips.append(norm_ip)
            else:
                details.append({"ip": ip, "deleted": 0, "error": "invalid_ip"})

        # Nếu không có IP hợp lệ, trả luôn
        if not valid_ips:
            return {"done": 0, "total": len(ips), "details": details, "applied": False, "redis_ok": True}

        # Nếu Redis đang DOWN -> fail-open: không thực hiện xoá, trả kết quả degrade
        if _redis_should_skip() or not _redis_health_check("unban_list_precheck"):
            # Redis không sẵn sàng
            for ip in valid_ips:
                details.append({"ip": ip, "deleted": 0, "status": "Không thể kết nối Redis, bỏ qua"})
            return {"done": 0, "total": len(ips), "details": details, "applied": False, "redis_ok": False}

        # Chia lô theo REDIS_BATCH_SIZE để tránh pipeline quá dài
        idx = 0
        while idx < len(valid_ips):
            batch = valid_ips[idx: idx + REDIS_BATCH_SIZE]
            idx += REDIS_BATCH_SIZE

            # Mỗi batch dùng pipeline
            with redis_client.pipeline(transaction=True) as p:
                for ip in batch:
                    p.delete(k_ban_ip(ip))

                results = _pipeline_exec_safe(p, desc="unban_list_pipeline")
            
            # Nếu batch này fail (Redis down trong lúc chạy) -> fail-open
            if results is None:
                for ip in batch:
                    details.append({"ip": ip, "deleted": 0, "status": "Không thể kết nối Redis, bỏ qua"})
                # Không break cứng, nhưng thường đã down thì các batch sau cũng sẽ skip do breaker
                continue

            # Ghép kết quả xoá cho từng IP trong batch
            for ip, r in zip(batch, results):
                d = int(r or 0)
                details.append({"ip": ip, "deleted": d, "status": "ok"})
                done += d


        return {
            "done": done,
            "total": len(ips),
            "details": details,
            "applied": True,
            "redis_ok": True
        }
    
    def get_ban_ttl (user_info: UserAuth, ip: str):
        """
        Trả TTL còn lại của một IP.

        Redis DOWN:
        - ttl_seconds=None
        - redis_ok=False
        """
        # Validate IP
        is_ip, norm_ip = _norm_ip(ip_raw = ip)

        if (not is_ip):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "Message": f"Địa chỉ IP không hợp lệ: {ip}",
                })
        
        # Kiểm tra quyền hạn (bắt buộc phải là Admin/Boss)
        if not (user_info["Privilege"] in HIGH_PRIVILEGE_LIST):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "Message": f"Bạn không có quyền thực hiện thao tác này",
                }
            )

        # Truy vấn thời gian còn lại
        ttl_val = _ban_ttl(norm_ip)
        return {
            "ip": ip,
            "ttl_seconds": ttl_val,           # None nếu Redis down
            "redis_ok": ttl_val is not None
        }
    
    def get_top_suspicious(user_info: UserAuth, limit:int):
        """
        Liệt kê top-N IP nghi vấn (cửa sổ 5 phút — key 'sus:ip:*:5min').
        - Quyền 403 nếu thiếu
        - Dùng SCAN + pipeline GET/TTL để tăng tốc
        - Sắp theo score giảm dần, rồi TTL (có → ưu tiên)
        - Trả {"count": ..., "items":[{"ip","score","ttl_seconds"}]}
        Redis DOWN:
        - trả items=[]
        - redis_ok=False
        """

        # Kiểm tra quyền hạn (bắt buộc phải là Admin/Boss)
        if not (user_info["Privilege"] in HIGH_PRIVILEGE_LIST):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "Message": f"Bạn không có quyền thực hiện thao tác này",
                }
            )
        
        # Nếu Redis đang down -> trả rỗng (fail-open)
        if _redis_should_skip() or not _redis_health_check("top_suspicious_precheck"):
            return {"count": 0, "items": [], "redis_ok": False}

        # Redis OK -> quét keys
        keys: List[Any] = _redis_safe(
            lambda: list(redis_client.scan_iter(match="sus:ip:*:5min", count=REDIS_SCAN_COUNT)),
            default=[],
            desc="scan_suspicious_keys"
        )

        if not keys:
            return {"count": 0, "items": [], "redis_ok": True}
        
        # Pipeline GET + TTL theo lô qua SCAN 
        items: List[dict] = []

        # Pipeline GET + TTL cho toàn bộ keys để giảm round-trip
        with redis_client.pipeline(transaction=False) as p:
            for k in keys:
                p.get(k)
                p.ttl(k)

            raw = _pipeline_exec_safe(p, desc="top_suspicious_pipeline")

        # Redis down giữa chừng -> trả rỗng
        if raw is None:
            return {"count": 0, "items": [], "redis_ok": False}
        
        # Parse kết quả GET/TTL theo cặp
        it = iter(raw)
        for k in keys:
            try:
                score_raw = next(it)
                ttl_raw = next(it)
            except StopIteration:
                break

            # k có thể là bytes hoặc str tuỳ cấu hình decode_responses → chuẩn hoá sang str
            k_str = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)

            prefix, suffix = "sus:ip:", ":5min"
            if not (k_str.startswith(prefix) and k_str.endswith(suffix)):
                continue # bảo vệ an toàn khi có key rác
            
            ip_str = k_str[len(prefix):-len(suffix)]

            try:
                score = int(score_raw or 0)
            except Exception:
                score = 0

            try:
                ttl_val = int(ttl_raw) if ttl_raw is not None else -2
            except Exception:
                ttl_val = -2

            items.append({
                "ip": ip_str,
                "score": score,
                "ttl_seconds": ttl_val if ttl_val > 0 else None
            })
        
        # Sort giảm dần theo score, TTL có giá trị ưu tiên hơn (None coi như 0)
        items.sort(key=lambda x: (x["score"], x["ttl_seconds"] or 0), reverse=True)

        # Giới hạn số lượng trả về
        limit = max(1, int(limit))
        return {"count": min(limit, len(items)), "items": items[:limit], "redis_ok": True}
    
    def get_current_ban(user_info: UserAuth):
        """
        Lấy danh sách các ip đang bị ban
        Redis DOWN:
        - items=[]
        - redis_ok=False
        """
        # Kiểm tra quyền hạn (bắt buộc phải là Admin/Boss)
        if not (user_info["Privilege"] in HIGH_PRIVILEGE_LIST):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "Message": f"Bạn không có quyền thực hiện thao tác này",
                }
            )
        
        # Redis down -> trả rỗng
        if _redis_should_skip() or not _redis_health_check("current_bans_precheck"):
            return {"items": [], "redis_ok": False}

        # Redis OK -> scan keys
        keys: List[Any] = _redis_safe(
            lambda: list(redis_client.scan_iter(match="ban:ip:*", count=REDIS_SCAN_COUNT)),
            default=[],
            desc="scan_ban_keys"
        )

        if not keys:
            return {"items": [], "redis_ok": True}
        
        # Pipeline TTL
        with redis_client.pipeline(transaction=False) as p:
            for k in keys:
                p.ttl(k)
            ttls = _pipeline_exec_safe(p, desc="current_bans_pipeline")

        if ttls is None:
            return {"items": [], "redis_ok": False}
        
        out: List[dict] = []

        for k, ttl in zip(keys, ttls):
            # Key -> str
            k_str = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)

            prefix = "ban:ip:"
            if not k_str.startswith(prefix):
                continue

            ip_str = k_str[len(prefix):]

            try:
                ttl_val = int(ttl)
            except Exception:
                continue

            if ttl_val > 0:
                out.append({"ip": ip_str, "ttl_seconds": ttl_val})

        # TTL nhỏ (sắp hết) đứng trước
        out.sort(key=lambda x: x["ttl_seconds"])
        return {"items": out, "redis_ok": True}
    