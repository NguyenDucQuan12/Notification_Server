from __future__ import annotations

"""
Các hàm tạo key cho Redis, để đảm bảo tính nhất quán và dễ quản lý.  
Dành cho các key liên quan đến stream notification, được phân loại theo tenant, user, job,... để dễ dàng subscribe và publish event.
Có thể thêm prefix chung nếu muốn, ví dụ: "noti:" để dễ dàng quản lý tất cả key liên quan đến notification trong Redis, nhưng ở đây tạm thời chưa có vì chưa thấy cần thiết.



Các server khác nhau (API service, worker service, notification service) khi cần tạo key liên quan đến stream notification thì đều dùng chung các hàm này để tạo key, đảm bảo tính nhất quán.
Copy tệp này vào tất cả các server (API service, worker service, notification service) để dùng chung hoặc có thể tạo 1 package riêng để cả 3 server cùng import dùng chung.
"""

# STREAM_PREFIX = getenv_str("STREAM_PREFIX", "prod_demo:stream")

def user_stream_key(tenant_id: str, user_id: str) -> str:
    """
    Stream riêng của một user.

    User thường sẽ subscribe vào stream này để nhận event của chính họ.
    """

    return f"stream:noti:tenant:{tenant_id}:user:{user_id}"


def tenant_stream_key(tenant_id: str) -> str:
    """
    Stream dashboard của một tenant.

    Admin/superuser dùng stream này để xem toàn bộ event trong tenant.
    """

    return f"stream:noti:tenant:{tenant_id}:dashboard"


def job_stream_key(tenant_id: str, job_id: str) -> str:
    """
    Stream riêng của một job.

    Dùng khi client mở màn hình chi tiết job và chỉ muốn nghe event của job đó.
    """

    return f"stream:noti:tenant:{tenant_id}:job:{job_id}"

def global_stream_key() -> str:
    """
    Stream toàn cục cho tất cả event của tất cả tenant.

    Dùng cho mục đích monitoring/logging, admin/superuser có thể subscribe để xem toàn bộ event hệ thống.
    """

    return f"stream:noti:global"

def ws_ticket_key(ticket: str) -> str:
    """
    Key lưu WebSocket ticket trong Redis.

    Ticket chỉ sống rất ngắn và dùng một lần.
    """

    return f"ws:ticket:{ticket}"