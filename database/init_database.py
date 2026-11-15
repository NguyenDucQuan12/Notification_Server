"""
Khởi tạo database mới cho Notification Server.

Các bước (chọn bằng tham số):
    --create-db   : chạy 00_create_database.sql trên [master]  (tạo DB nếu chưa có, bật RCSI...)
    --reset       : chạy 01_create_tables.sql                   (XÓA và tạo lại toàn bộ bảng)
    (mặc định)    : chạy 02_seed_data.sql                       (role admin/user + tài khoản admin)
    --clear-redis : xóa thông báo cũ còn trong Redis (stream:noti:*, ticket, cache xác thực)

Cấu hình kết nối lấy từ env/.env.common + env/.env.api (giống API Server).

Ví dụ:
    # Database MỚI hoàn toàn, xóa sạch thông báo cũ trong cả SQL lẫn Redis:
    python database\\init_database.py --create-db --reset --clear-redis --username quan

    # Chỉ thêm/cập nhật role và tài khoản admin (không xóa gì):
    python database\\init_database.py --username admin2

Mật khẩu: truyền --password, hoặc biến môi trường NOTI_ADMIN_PASSWORD, hoặc nhập khi được hỏi (khuyến nghị).
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import sys
from pathlib import Path

import pyodbc
import redis

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
sys.path.insert(0, str(PROJECT_ROOT / "fastapi_server" / "src"))

from config.config import settings  # noqa: E402  (nạp env/.env.common + env/.env.api)
from auth.hash import Hash  # noqa: E402  (bcrypt - cùng hàm API dùng khi đăng nhập)

GO_RE = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)
BODY_MARKER = "-- ==================================== BODY"


def connection_string(database: str) -> str:
    """
    Chuẩn bị chuỗi kết nối ODBC cho pyodbc.
    """
    return (
        f"DRIVER={{{settings.sql_server_driver}}};"
        f"SERVER={settings.db_host};"   # giống hệt db/database.py của API
        f"DATABASE={database};"
        f"UID={settings.db_user};PWD={{{settings.db_password}}};"
        f"TrustServerCertificate={settings.trust_db_server};"
    )


def run_batch(cursor: pyodbc.Cursor, sql: str, params: tuple = ()) -> None:
    """
    Chạy 1 batch và đọc hết các result set (lỗi ở câu lệnh sau chỉ lộ ra khi gọi nextset)
    """
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    while True:
        if cursor.description:
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            if rows:
                print("   " + " | ".join(cols))
                for row in rows:
                    print("   " + " | ".join("" if v is None else str(v) for v in row))
        for _, message in getattr(cursor, "messages", None) or []:
            # Bỏ tiền tố "[ODBC Driver 17 for SQL Server][SQL Server]" trước thông báo PRINT
            print("   " + re.sub(r"^(\[[^\]]*\])+\s*", "", str(message)))
        if not cursor.nextset():
            break


def run_file(database: str, path: Path, *, rename_db: str | None = None) -> None:
    """
    Chạy 1 file SQL (có thể có nhiều batch GO) trên database.
    Nếu rename_db != None thì thay tất cả FastAPI -> rename_db trong file trước khi chạy.
    """
    sql = path.read_text(encoding="utf-8")
    if rename_db:
        sql = re.sub(r"\bFastAPI\b", rename_db, sql)
    print(f"\n>>> {path.name}  (database: {database})")
    with pyodbc.connect(connection_string(database), autocommit=True) as conn:
        cursor = conn.cursor()
        for batch in GO_RE.split(sql):
            if batch.strip():
                run_batch(cursor, batch)


def seed(database: str, *, tenant: str, username: str, password: str, full_name: str, email: str | None) -> None:
    """
    Chạy 02_seed_data.sql để tạo role admin/user và tài khoản admin.
    Tham số tenant/username/password/full_name/email sẽ thay thế các biến trong file SQL.
    """
    sql = (HERE / "02_seed_data.sql").read_text(encoding="utf-8")
    body = sql[sql.index(BODY_MARKER):]
    header = (
        "SET NOCOUNT ON; SET XACT_ABORT ON;\n"
        "DECLARE @tenant_id VARCHAR(64) = ?, @username VARCHAR(128) = ?, @password_hash VARCHAR(512) = ?,\n"
        "        @full_name NVARCHAR(255) = ?, @email VARCHAR(255) = ?;\n"
    )
    print(f"\n>>> 02_seed_data.sql  (database: {database})")
    with pyodbc.connect(connection_string(database), autocommit=True) as conn:
        run_batch(conn.cursor(), header + body,
                  (tenant, username, Hash.bcrypt(password), full_name, email))


def clear_redis() -> None:
    """
    Xóa các thông báo cũ còn trong Redis (stream:noti:*, ticket, cache xác thực).
    """
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    total = 0
    for pattern in ("stream:noti:*", "ws:ticket:*", "auth:user_ok:*", "noti:outbox:wakeup"):
        batch: list[str] = []
        for key in r.scan_iter(pattern, count=1000):
            batch.append(key)
            if len(batch) >= 500:
                total += r.delete(*batch)
                batch.clear()
        if batch:
            total += r.delete(*batch)
    print(f"\n>>> Redis ({settings.redis_url}): đã xóa {total} key thông báo cũ")


def main() -> None:
    """
    Chạy init_database.py để tạo database + bảng + role + tài khoản admin.
    """
    p = argparse.ArgumentParser(description="Khởi tạo database Notification Server.")
    p.add_argument("--database", default=settings.db_name, help="Tên database (mặc định SQL_SERVER_DATABASE trong env).")
    p.add_argument("--create-db", action="store_true", help="Tạo database + bật RCSI (chạy trên master).")
    p.add_argument("--reset", action="store_true", help="XÓA và tạo lại toàn bộ bảng.")
    p.add_argument("--yes", action="store_true", help="Không hỏi xác nhận khi --reset.")
    p.add_argument("--clear-redis", action="store_true", help="Xóa thông báo cũ trong Redis.")
    p.add_argument("--skip-seed", action="store_true", help="Không tạo role / tài khoản admin.")
    p.add_argument("--tenant", default="system")
    p.add_argument("--username", default="quan")
    p.add_argument("--password", default=os.getenv("NOTI_ADMIN_PASSWORD"))
    p.add_argument("--full-name", default="Quản trị hệ thống")
    p.add_argument("--email", default=None)
    args = p.parse_args()

    print(f"SQL Server: {settings.db_host}  database: {args.database}  user: {settings.db_user}")

    if args.reset and not args.yes:
        answer = input(f"!!! --reset sẽ XÓA TOÀN BỘ bảng và dữ liệu trong [{args.database}].\n"
                       f"    Gõ lại tên database để xác nhận: ").strip()
        if answer != args.database:
            sys.exit("Đã hủy.")

    password = args.password
    if not args.skip_seed and not password:
        password = getpass.getpass(f"Mật khẩu cho tài khoản admin '{args.username}': ")
        if password != getpass.getpass("Nhập lại mật khẩu: "):
            sys.exit("Mật khẩu nhập lại không khớp.")
    if not args.skip_seed and len(password) < 6:
        sys.exit("Mật khẩu phải có ít nhất 6 ký tự.")

    if args.create_db:
        run_file("master", HERE / "00_create_database.sql",
                 rename_db=None if args.database == "FastAPI" else args.database)
    if args.reset:
        run_file(args.database, HERE / "01_create_tables.sql")
    if not args.skip_seed:
        seed(args.database, tenant=args.tenant, username=args.username, password=password,
             full_name=args.full_name, email=args.email)
    if args.clear_redis:
        clear_redis()

    print("\nHoàn tất.")


if __name__ == "__main__":
    main()
