/* =====================================================================================
   02_seed_data.sql - DỮ LIỆU BAN ĐẦU
   -------------------------------------------------------------------------------------
   Tạo:
     1. Role "admin"  : toàn quyền (kể cả gửi thông báo cho người khác: notification:send)
     2. Role "user"   : người dùng thường (nhận thông báo, theo dõi job của mình)
     3. 1 tài khoản admin (superuser) thuộc tenant "system"
   KHÔNG tạo thông báo nào: các bảng notification_* để trống.

   Chạy lại nhiều lần an toàn: role đã có thì cập nhật quyền, username đã có thì bỏ qua.

   CÁCH 1 (khuyến nghị): python database\init_database.py --reset
       -> script Python tự mã hóa mật khẩu bằng bcrypt rồi chạy phần thân file này.

   CÁCH 2 (SSMS): sửa các biến ở mục "THAM SỐ" rồi Execute.
       Mật khẩu KHÔNG được lưu dạng chữ thường, phải là bcrypt hash. Tạo hash bằng:
         .noti_server_venv\Scripts\python.exe -c "import bcrypt;print(bcrypt.hashpw(b'MatKhauCuaBan',bcrypt.gensalt()).decode())"
   ===================================================================================== */

SET NOCOUNT ON;
SET XACT_ABORT ON;   -- lỗi bất kỳ -> rollback toàn bộ, không để dữ liệu nửa vời

-- ==================================== THAM SỐ ========================================
DECLARE @tenant_id      VARCHAR(64)   = 'system';           -- login hiện cố định tenant "system"
DECLARE @username       VARCHAR(128)  = 'quan';
DECLARE @password_hash  VARCHAR(512)  = '<DAN_BCRYPT_HASH_VAO_DAY>';
DECLARE @full_name      NVARCHAR(255) = N'Nguyễn Đức Quân';
DECLARE @email          VARCHAR(255)  = NULL;
-- ==================================== BODY ===========================================
-- (init_database.py thay khối THAM SỐ ở trên bằng tham số của nó và chạy từ dòng này trở xuống)

IF @password_hash IS NULL OR @password_hash NOT LIKE '$2%'
    THROW 50001, N'@password_hash phải là bcrypt hash (bắt đầu bằng $2b$...). Xem hướng dẫn ở đầu file.', 1;

DECLARE @now DATETIMEOFFSET = SYSDATETIMEOFFSET();

BEGIN TRANSACTION;

-- ------------------------------------------------------------------ 1-2. Roles
-- MERGE: chưa có thì thêm, có rồi thì cập nhật danh sách quyền (đồng bộ khi thêm quyền mới).
-- HOLDLOCK: tránh 2 lần chạy song song cùng chèn 1 role.
MERGE dbo.roles WITH (HOLDLOCK) AS target
USING (VALUES
    ('admin', 'Admin', N'Quản trị hệ thống',
     '["notification:subscribe","notification:tenant_dashboard","notification:job_watch","notification:global_subscribe","notification:send","role:create","role:read","role:update","role:delete","user:create","user:read","user:update","job:create","job:read","job:read_all","job:download"]'),
    ('user',  'User',  N'Người dùng thường',
     '["notification:subscribe","notification:job_watch","job:create","job:read","job:download"]')
) AS source (role_id, role_name, description, permissions_json)
ON target.role_id = source.role_id
WHEN MATCHED THEN UPDATE SET
    role_name = source.role_name,
    description = source.description,
    permissions_json = source.permissions_json,
    is_active = 1,
    updated_at = @now
WHEN NOT MATCHED THEN INSERT (role_id, role_name, description, permissions_json, is_active, created_at, updated_at)
    VALUES (source.role_id, source.role_name, source.description, source.permissions_json, 1, @now, @now);

-- ------------------------------------------------------------------ 3. Tài khoản admin
IF NOT EXISTS (SELECT 1 FROM dbo.user_auth WHERE tenant_id = @tenant_id AND username = @username)
BEGIN
    -- user_id dạng 32 ký tự hex (giống uuid4().hex mà API sinh ra)
    DECLARE @user_id VARCHAR(64) = LOWER(REPLACE(CONVERT(VARCHAR(36), NEWID()), '-', ''));

    INSERT INTO dbo.users (user_id, tenant_id, full_name, email, phone, is_active, created_at, updated_at)
    VALUES (@user_id, @tenant_id, @full_name, @email, NULL, 1, @now, @now);

    INSERT INTO dbo.user_auth (tenant_id, user_id, role_id, username, password_hash,
                               is_superuser, is_login_enabled, failed_login_count,
                               last_login_at, password_changed_at, created_at, updated_at)
    VALUES (@tenant_id, @user_id, 'admin', @username, @password_hash,
            1, 1, 0, NULL, @now, @now, @now);

    PRINT N'Đã tạo tài khoản admin: ' + @username + N' (user_id=' + @user_id + N')';
END
ELSE
    PRINT N'Tài khoản ' + @username + N' đã tồn tại, bỏ qua.';

COMMIT TRANSACTION;

-- ------------------------------------------------------------------ Kiểm tra
SELECT role_id, role_name, permissions_json FROM dbo.roles ORDER BY role_id;
SELECT a.tenant_id, a.username, a.user_id, a.role_id, a.is_superuser, a.is_login_enabled, u.full_name
FROM dbo.user_auth AS a JOIN dbo.users AS u ON u.user_id = a.user_id;
