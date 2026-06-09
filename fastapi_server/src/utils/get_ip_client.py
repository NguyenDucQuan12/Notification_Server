from fastapi import Request
from ipaddress import ip_address
from typing import Optional

def get_client_ip(request: Request) -> str:
    """
    Nhận 1 request từ FastAPI và trả về địa chỉ IP của client  
    - Nếu có X-Forwarded-For: lấy phần tử đầu (client gốc). Vì nếu triển khai trên Nginx thì ưu tiên X-Forwarded-For
    - Else: request.client.host
    """
    # Lưu ý một số nginx có thể set header là "X-real-ip", cần thay đổi cho đúng
    xff = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
    if xff:
        # format: "client, proxy1, proxy2"
        return xff.split(",")[0].strip()
    
    client = request.client
    return client.host if client else "Unknown"

def norm_ip(ip_raw: Optional[str]) -> str:
    """
    Chuẩn hoá chuỗi IP về dạng hợp lệ; nếu lỗi, trả '-'
    """
    # Kiểm tra giá trị truyền vào tồn tại hay không và có phải là chuỗi string hay không
    if not ip_raw or not isinstance(ip_raw, str):
        return False, None
    
    try:
        ip_address(ip_raw)  # Parse IPv4/IPv6; sai sẽ ném ValueError

        return True, str(ip_address(ip_raw))
    except Exception:
        # Nếu không parse được, trả nguyên để không crash
        return False, ip_raw