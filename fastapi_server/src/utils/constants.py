"""
File chứa các hằng số được sử dụng trong toàn bộ ứng dụng FastAPI, giúp quản lý và bảo trì dễ dàng hơn.
Các hằng số này có thể bao gồm:
- Biểu thức chính quy (regex) để kiểm tra định dạng email.
- Đường dẫn mặc định cho avatar người dùng.
- Danh sách các quyền hạn (privilege) có thể gán cho người dùng.
Việc sử dụng hằng số giúp tránh việc lặp lại cùng một giá trị ở nhiều nơi trong code, đồng thời giúp dễ dàng thay đổi giá trị khi cần thiết mà không phải sửa nhiều chỗ.
"""

import os
from typing_extensions import Literal


EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
# Regex kiểm tra role_id.
# Cho phép chữ, số, dấu gạch dưới, gạch ngang, dấu chấm và dấu hai chấm.
# Ví dụ hợp lệ:
# - admin
# - staff
# - job:create
# - system.admin
ROLE_ID_REGEX = r"^[a-zA-Z0-9_.:-]+$"


# Định nghĩa các trạng thái của công việc (job) trong hệ thống. Các trạng thái này có thể được sử dụng để theo dõi tiến trình của các tác vụ như gửi thông báo, xử lý dữ liệu, v.v.
JobStatus = Literal[
    "queued",
    "processing",
    "success",
    "failed",
    "cancelled",
]


DEFAULT_AVATAR = "images/default/avatar.png"

# Danh sách các quyền hạn
DEFAULT_PRIVILEGE = "Guest"
BOSS_PRIVILEGE = "Boss"
HIGH_PRIVILEGE_LIST = ["Admin", "Boss"]
PRIVILEGE_LIST = ["Guest", "User", "Admin"]
FULL_PRIVILEGE_LIST = ["Guest", "User", "Admin", "Boss"]


# Các định nghĩa về thông báo (notification) và sự kiện (event) trong hệ thống.
SCHEMA_VERSION = "1.0"
# Số stream đích (outbox) tối đa mà API publish NGAY trong request (chế độ DIRECT).
# Vượt ngưỡng -> chỉ ghi outbox, Outbox Worker publish theo lô ở nền.
# Tránh request HTTP bị treo lâu và chiếm DB pool khi gửi cho hàng nghìn người.
DIRECT_PUBLISH_MAX_OUTBOX = int(os.getenv("DIRECT_PUBLISH_MAX_OUTBOX", "200"))
