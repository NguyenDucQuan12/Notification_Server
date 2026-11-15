/* =====================================================================================
   01_create_tables.sql - TẠO TOÀN BỘ BẢNG CHO DATABASE
   -------------------------------------------------------------------------------------
   !!! CẢNH BÁO: phần 1 XÓA (DROP) toàn bộ bảng và DỮ LIỆU hiện có. !!!

   Cách chạy (chọn 1):
     - SSMS        : mở file, chọn đúng database FastAPI, bấm Execute.
     - sqlcmd      : sqlcmd -S localhost -d FastAPI -U <user> -P <pass> -C -f 65001 -i database\01_create_tables.sql
     - Python      : python database\init_database.py --reset   (chạy file này + dữ liệu ban đầu)
   ===================================================================================== */

-- Bắt buộc cho FILTERED INDEX (index có WHERE): các SET này phải ON khi tạo index và khi ghi dữ liệu.
-- ODBC / pyodbc / SSMS mặc định đã ON; ghi rõ ở đây để chạy bằng công cụ khác cũng đúng.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

/* =====================================================================================
   1. XÓA BẢNG CŨ NẾU TỒN TẠI (nếu có)
   ===================================================================================== */
DROP TABLE IF EXISTS dbo.notification_outbox;
DROP TABLE IF EXISTS dbo.notification_deliveries;
DROP TABLE IF EXISTS dbo.notification_recipients;
DROP TABLE IF EXISTS dbo.notifications;
DROP TABLE IF EXISTS dbo.job_record;
DROP TABLE IF EXISTS dbo.user_auth;
DROP TABLE IF EXISTS dbo.users;
DROP TABLE IF EXISTS dbo.roles;
GO

/* =====================================================================================
   2. TẠO BẢNG + INDEX
   ===================================================================================== */
-- -------------------------------------------------------------------------------------
-- 2.1. roles: Vai trò + danh sách quyền (permissions_json). Bảng nhỏ, ít thay đổi.
-- -------------------------------------------------------------------------------------
CREATE TABLE roles (
    id INTEGER NOT NULL IDENTITY,
    role_id VARCHAR(64) NOT NULL,
    role_name VARCHAR(64) NOT NULL,
    description NVARCHAR(500) NULL,
    permissions_json VARCHAR(max) NOT NULL,
    is_active BIT NOT NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_roles_role_id ON roles (role_id);
CREATE UNIQUE INDEX ix_roles_role_name ON roles (role_name);
GO

-- -------------------------------------------------------------------------------------
-- 2.2. users: Hồ sơ người dùng (tên, email...). Tách khỏi user_auth để thông tin đăng nhập không lẫn với hồ sơ.
-- -------------------------------------------------------------------------------------
CREATE TABLE users (
    id INTEGER NOT NULL IDENTITY,
    user_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NULL,
    full_name NVARCHAR(255) NULL,
    email VARCHAR(255) NULL,
    phone VARCHAR(32) NULL,
    is_active BIT NOT NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX ix_users_email ON users (email);
CREATE INDEX ix_users_tenant_id ON users (tenant_id);
CREATE UNIQUE INDEX ix_users_user_id ON users (user_id);
GO

-- -------------------------------------------------------------------------------------
-- 2.3. user_auth: Thông tin đăng nhập: username (duy nhất trong tenant), password_hash (bcrypt), quyền superuser.
-- -------------------------------------------------------------------------------------
CREATE TABLE user_auth (
    id INTEGER NOT NULL IDENTITY,
    tenant_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    role_id VARCHAR(64) NULL,
    username VARCHAR(128) NOT NULL,
    password_hash VARCHAR(512) NOT NULL,
    is_superuser BIT NOT NULL,
    is_login_enabled BIT NOT NULL,
    failed_login_count INTEGER NOT NULL,
    last_login_at DATETIMEOFFSET NULL,
    password_changed_at DATETIMEOFFSET NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_user_auth_tenant_username UNIQUE (tenant_id, username)
);
CREATE INDEX ix_user_auth_role_id ON user_auth (role_id);
CREATE UNIQUE INDEX ix_user_auth_user_id ON user_auth (user_id);
GO

-- -------------------------------------------------------------------------------------
-- 2.4. job_record: Công việc xử lý file (dành cho luồng upload/job).
-- -------------------------------------------------------------------------------------
CREATE TABLE job_record (
    job_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    filename NVARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    progress INTEGER NOT NULL,
    message NVARCHAR(255) NOT NULL,
    attempts INTEGER NOT NULL,
    upload_object_key VARCHAR(max) NOT NULL,
    result_object_key VARCHAR(max) NULL,
    error NVARCHAR(500) NULL,
    trace_id VARCHAR(64) NOT NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    finished_at DATETIMEOFFSET NULL,
    PRIMARY KEY (job_id)
);
CREATE INDEX ix_job_record_status ON job_record (status);
CREATE INDEX ix_job_record_tenant_id ON job_record (tenant_id);
CREATE INDEX ix_job_record_trace_id ON job_record (trace_id);
CREATE INDEX ix_job_record_user_id ON job_record (user_id);
GO

-- -------------------------------------------------------------------------------------
-- 2.5. notifications: Nội dung CHUNG của 1 thông báo (1 dòng / thông báo, dù gửi cho bao nhiêu người).
-- -------------------------------------------------------------------------------------
CREATE TABLE notifications (
    id INTEGER NOT NULL IDENTITY,
    notification_id VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    schema_version VARCHAR(20) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    notification_type VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    source_service VARCHAR(100) NOT NULL,
    source_instance VARCHAR(100) NULL,
    environment VARCHAR(32) NOT NULL,
    actor_type VARCHAR(32) NULL,
    actor_id VARCHAR(64) NULL,
    actor_name NVARCHAR(255) NULL,
    actor_avatar_url VARCHAR(2048) NULL,
    resource_type VARCHAR(64) NULL,
    resource_id VARCHAR(128) NULL,
    resource_version INTEGER NULL,
    audience_type VARCHAR(32) NOT NULL,
    audience_json NVARCHAR(max) NOT NULL,
    channels_json NVARCHAR(max) NOT NULL,
    template_key VARCHAR(128) NULL,
    template_version VARCHAR(32) NULL,
    locale VARCHAR(20) NOT NULL,
    title NVARCHAR(255) NOT NULL,
    body NVARCHAR(max) NOT NULL,
    short_body NVARCHAR(500) NULL,
    image_url VARCHAR(2048) NULL,
    variables_json NVARCHAR(max) NOT NULL,
    action_type VARCHAR(32) NOT NULL,
    action_target VARCHAR(2048) NULL,
    action_label NVARCHAR(100) NULL,
    action_method VARCHAR(10) NOT NULL,
    action_open_new_tab BIT NOT NULL,
    data_json NVARCHAR(max) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    deduplication_key VARCHAR(255) NULL,
    deduplication_window_seconds INTEGER NULL,
    delivery_ttl_seconds INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    retry_delay_seconds INTEGER NOT NULL,
    require_ack BIT NOT NULL,
    correlation_id VARCHAR(128) NULL,
    trace_id VARCHAR(128) NULL,
    causation_id VARCHAR(128) NULL,
    status VARCHAR(20) NOT NULL,
    available_at DATETIMEOFFSET NOT NULL,
    expires_at DATETIMEOFFSET NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT ck_notifications_status CHECK (status IN ('active', 'cancelled', 'expired'))
);
CREATE INDEX ix_notifications_deduplication ON notifications (tenant_id, deduplication_key, created_at);
CREATE UNIQUE INDEX ix_notifications_event_id ON notifications (event_id);
CREATE INDEX ix_notifications_expires_at ON notifications (expires_at);
CREATE UNIQUE INDEX ix_notifications_notification_id ON notifications (notification_id);
CREATE INDEX ix_notifications_resource ON notifications (tenant_id, resource_type, resource_id);
CREATE INDEX ix_notifications_tenant_created ON notifications (tenant_id, created_at);
CREATE INDEX ix_notifications_tenant_type ON notifications (tenant_id, notification_type);
GO

-- -------------------------------------------------------------------------------------
-- 2.6. notification_recipients: Trạng thái ĐỌC của từng người nhận (1 dòng / người / thông báo). Phục vụ hộp thư + badge chưa đọc.
-- -------------------------------------------------------------------------------------
CREATE TABLE notification_recipients (
    id INTEGER NOT NULL IDENTITY,
    notification_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    recipient_user_id VARCHAR(64) NOT NULL,
    state VARCHAR(20) NOT NULL,
    is_pinned BIT NOT NULL,
    delivered_at DATETIMEOFFSET NULL,
    seen_at DATETIMEOFFSET NULL,
    read_at DATETIMEOFFSET NULL,
    archived_at DATETIMEOFFSET NULL,
    deleted_at DATETIMEOFFSET NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_notification_recipient UNIQUE (notification_id, recipient_user_id),
    CONSTRAINT ck_notification_recipients_state CHECK (state IN ('unread', 'seen', 'read', 'archived', 'deleted'))
);
CREATE INDEX ix_notification_recipients_unread ON notification_recipients (tenant_id, recipient_user_id) WHERE state = 'unread' AND deleted_at IS NULL;
CREATE INDEX ix_notification_recipients_user_created ON notification_recipients (tenant_id, recipient_user_id, created_at);
GO

-- -------------------------------------------------------------------------------------
-- 2.7. notification_deliveries: Trạng thái GỬI theo từng kênh (1 dòng / người / kênh: in_app, websocket...).
-- -------------------------------------------------------------------------------------
CREATE TABLE notification_deliveries (
    id INTEGER NOT NULL IDENTITY,
    delivery_id VARCHAR(64) NOT NULL,
    notification_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    recipient_user_id VARCHAR(64) NOT NULL,
    channel VARCHAR(32) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider VARCHAR(100) NULL,
    provider_message_id VARCHAR(255) NULL,
    attempt_count INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    next_retry_at DATETIMEOFFSET NULL,
    error_code VARCHAR(100) NULL,
    error_message NVARCHAR(max) NULL,
    queued_at DATETIMEOFFSET NULL,
    sent_at DATETIMEOFFSET NULL,
    delivered_at DATETIMEOFFSET NULL,
    failed_at DATETIMEOFFSET NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_notification_delivery_channel UNIQUE (notification_id, recipient_user_id, channel),
    CONSTRAINT ck_notification_deliveries_status CHECK (status IN ('pending', 'queued', 'sending', 'sent', 'delivered', 'failed', 'expired', 'cancelled'))
);
CREATE UNIQUE INDEX ix_notification_deliveries_delivery_id ON notification_deliveries (delivery_id);
CREATE INDEX ix_notification_deliveries_recipient ON notification_deliveries (tenant_id, recipient_user_id, created_at);
CREATE INDEX ix_notification_deliveries_status_retry ON notification_deliveries (status, next_retry_at);
GO

-- -------------------------------------------------------------------------------------
-- 2.8. notification_outbox: Hàng đợi bền vững để publish vào Redis Stream (Transactional Outbox). 1 dòng / stream đích.
-- -------------------------------------------------------------------------------------
CREATE TABLE notification_outbox (
    id INTEGER NOT NULL IDENTITY,
    outbox_id VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    stream_scope VARCHAR(20) NOT NULL,
    target_tenant_id VARCHAR(64) NULL,
    target_user_id VARCHAR(64) NULL,
    target_job_id VARCHAR(64) NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    stream_key VARCHAR(255) NOT NULL,
    payload_json NVARCHAR(max) NOT NULL,
    status VARCHAR(20) NOT NULL,
    attempt_count INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    available_at DATETIMEOFFSET NOT NULL,
    locked_at DATETIMEOFFSET NULL,
    locked_by VARCHAR(100) NULL,
    published_stream_id VARCHAR(100) NULL,
    published_at DATETIMEOFFSET NULL,
    last_error NVARCHAR(max) NULL,
    created_at DATETIMEOFFSET NOT NULL,
    updated_at DATETIMEOFFSET NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uq_notification_outbox_event_stream UNIQUE (event_id, stream_key),
    CONSTRAINT ck_notification_outbox_status CHECK (status IN ('pending', 'processing', 'published', 'failed')),
    CONSTRAINT ck_notification_outbox_scope CHECK (stream_scope IN ('user', 'tenant', 'job', 'global'))
);
CREATE INDEX ix_notification_outbox_aggregate_id ON notification_outbox (aggregate_id);
CREATE UNIQUE INDEX ix_notification_outbox_outbox_id ON notification_outbox (outbox_id);
CREATE INDEX ix_notification_outbox_pending ON notification_outbox (available_at, id) INCLUDE (attempt_count, max_attempts) WHERE status = 'pending';
GO

/* =====================================================================================
   3. KIỂM TRA: liệt kê bảng và index vừa tạo
   ===================================================================================== */
SELECT t.name AS table_name, i.name AS index_name, i.type_desc, i.is_unique, i.has_filter, i.filter_definition
FROM sys.tables AS t
JOIN sys.indexes AS i ON i.object_id = t.object_id AND i.type > 0
ORDER BY t.name, i.index_id;
GO
