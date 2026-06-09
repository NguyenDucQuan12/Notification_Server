# tools/test_ban_unban.py
# -*- coding: utf-8 -*-

"""
Script test:
1) Đăng nhập hệ thống qua /auth/login (form-urlencoded) để lấy Access_token (JWT của bạn).
2) Bắn nhiều request nghi vấn (SQLi/XSS) vào path mục tiêu để bị BAN.
3) Khi thấy 403 -> gọi /security/admin/current_bans để lấy IP vừa bị BAN (TTL lớn nhất).
4) Gỡ ban qua /security/admin/unban (Bearer token).
5) Gọi lại endpoint -> kỳ vọng KHÔNG còn 403.

Yêu cầu:
- Tài khoản dùng login phải có Privilege 'Admin' hoặc 'Boss' (để gọi /security/admin/*).
- Server đã mount security_guard + security_admin router như bạn cấu hình.
"""
import requests                    # Gửi HTTP đồng bộ (pip install requests)
import time                        # time.sleep giữa các lần bắn
from typing import Optional, Tuple, Dict, Any  # Type hint nhẹ


# ---------- Helpers gọi API ----------

def login_and_get_token(base_url: str, username: str, password: str) -> str:
    """
    Đăng nhập qua /auth/login (form-urlencoded) -> nhận token (Access_token).
    Server của bạn trả về {"Access_token": "...", "Token_type": "Bearer"}.
    """
    url = f"{base_url}/auth/login"
    data = {"username": username, "password": password}        # OAuth2PasswordRequestForm
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(url, data=data, headers=headers, timeout=10)
    if not r.ok:
        # Thử hiển thị lỗi có cấu trúc
        try:
            raise SystemExit(f"[LOGIN FAIL] {r.status_code} {r.json()}")
        except Exception:
            raise SystemExit(f"[LOGIN FAIL] {r.status_code} {r.text}")
    j = r.json()
    token = j.get("Access_token") or j.get("access_token")
    print(f"Token: {token}")
    if not token:
        raise SystemExit("[LOGIN FAIL] Không tìm thấy Access_token trong đáp ứng /auth/login")
    return token


def auth_headers(token: str) -> Dict[str, str]:
    """Tạo header Authorization: Bearer <token> để gọi /security/admin/*"""
    return {"Authorization": f"Bearer {token}"}


def fire_suspicious(base_url: str, path: str, n: int, sleep_ms: int = 50) -> Tuple[bool, Optional[int]]:
    """
    Bắn N request có query nghi vấn để bị cộng điểm & dẫn tới BAN.
    Trả về (banned_flag, last_status_code).
    """
    target = f"{base_url.rstrip('/')}{path}"
    banned = False
    last_code = None

    print(f"[1] Gửi {n} request nghi vấn đến {target}")
    # Mẫu payload cố ý “xấu” để tăng điểm nghi vấn (trùng logic middleware)
    payloads = [
        "' OR 1=1 -- <script>alert(1)</script>",
        "javascript:alert(1)",
        "union select username, password from users",
        "../etc/passwd"
    ]

    for i in range(n):
        q = payloads[i % len(payloads)]
        url = f"{target}?q={q}"
        try:
            r = requests.get(url, timeout=5)
            last_code = r.status_code
            print(f"  - {i+1:03d}/{n}: {last_code}")
            if r.status_code == 403:
                print("  -> ĐÃ BỊ BAN (403).")
                banned = True
                break
        except Exception as e:
            print(f"  - {i+1:03d}/{n}: lỗi {e}")
        time.sleep(sleep_ms / 1000.0)

    # Nếu chưa 403, bắn thêm ít request nhanh để đủ ngưỡng
    if not banned:
        print("[!] Chưa thấy 403. Thử gửi thêm 20 lần ...")
        for i in range(20):
            url = f"{target}?q=' OR 1=1 --"
            try:
                r = requests.get(url, timeout=5)
                last_code = r.status_code
                print(f"  - Thêm {i+1:02d}: {r.status_code}")
                if r.status_code == 403:
                    print("  -> ĐÃ BỊ BAN (403).")
                    banned = True
                    break
            except Exception as e:
                print(f"  - Thêm {i+1:02d}: lỗi {e}")
            time.sleep(sleep_ms / 1000.0)

    return banned, last_code


def pick_banned_ip(base_url: str, token: str) -> Optional[str]:
    """
    Đọc danh sách IP bị ban từ /security/admin/current_bans (yêu cầu quyền Admin/Boss).
    Chọn IP có TTL lớn nhất (thường là IP vừa bị ban gần nhất).
    """
    url = f"{base_url}/security/admin/current_bans"
    r = requests.get(url, headers=auth_headers(token), timeout=10)
    if not r.ok:
        print(f"[!] current_bans FAIL {r.status_code}: {r.text}")
        return None
    items = (r.json() or {}).get("items", [])
    if not items:
        print("[i] Không có IP nào đang bị BAN.")
        return None
    # Chọn IP có TTL lớn nhất
    ip = max(items, key=lambda x: x.get("ttl_seconds", 0)).get("ip")
    print(f"[i] Chọn IP bị BAN để gỡ: {ip}")
    return ip


def unban_ip(base_url: str, token: str, ip: str) -> bool:
    """
    Gọi /security/admin/unban để gỡ BAN một IP.
    Trả True nếu xoá được key (deleted=1).
    """
    url = f"{base_url}/security/admin/unban"
    r = requests.post(url, headers=auth_headers(token), json={"ip": ip}, timeout=10)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    ok = r.ok and (body.get("deleted", 0) == 1)
    print(f"[UNBAN] {r.status_code} {body}")
    return ok


# ---------- Main ----------
base = "http://127.0.0.1:8000" # Base URL của API FastAPI
username = "nguyenducquan2001@gmail.com"
password = "1"
path = "/file"  # Đường dẫn sẽ gọi để gây nghi vấn
count = 50  # Số request nghi vấn gửi liên tiếp
sleep_ms = 50 # Nghỉ giữa các lần bắn (ms)

def main():
    # 2) Login để lấy token (Bearer)
    print("[LOGIN] Đăng nhập /auth/login ...")
    token = login_and_get_token(base, username, password)

    # 3) Gây BAN bằng cách bắn nhiều request nghi vấn
    banned, last_code = fire_suspicious(base, path, count, sleep_ms)

    if not banned:
        print("[KQ] Chưa bị BAN (không thấy 403). Có thể tăng --count hoặc giảm ngưỡng BAN_RULE.suspicious_per_5min.")
        return

    # 4) Tìm IP vừa bị BAN (TTL lớn nhất)
    ip = pick_banned_ip(base, token)
    if not ip:
        print("[!] Không tìm thấy IP trong danh sách BAN. Kiểm tra lại middleware/redis.")
        return

    # 5) Gỡ BAN IP
    if not unban_ip(base, token, ip):
        print("[!] UNBAN thất bại. Kiểm tra quyền Admin/Boss và router /security/admin/unban.")
        return

    # 6) Thử gọi lại endpoint sau khi gỡ BAN
    target = f"{base}{path}"
    print("[RETRY] Gọi lại endpoint sau khi UNBAN ...")
    try:
        r2 = requests.get(target, timeout=5)
        print(f"   - Status sau UNBAN: {r2.status_code}")
        if r2.status_code != 403:
            print("   -> OK: đã gỡ BAN thành công.")
        else:
            print("   -> Vẫn bị BAN: kiểm tra lại key ban hoặc IP mà server nhìn thấy.")
    except Exception as e:
        print("   - Lỗi khi retry:", e)

def get_current_ban():
    token = login_and_get_token(base, username, password)
    ip = pick_banned_ip(base, token)
    print(ip)

if __name__ == "__main__":
    get_current_ban()
