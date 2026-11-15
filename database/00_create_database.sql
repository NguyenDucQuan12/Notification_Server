/* =====================================================================================
   00_create_database.sql - TẠO DATABASE VÀ CẤU HÌNH CHỊU TẢI
   -------------------------------------------------------------------------------------
   Chạy trên database [master] bằng tài khoản có quyền tạo database (sysadmin / dbcreator).
   Chạy 1 lần. Nếu database đã tồn tại thì chỉ áp dụng lại cấu hình.

     sqlcmd -S localhost -d master -U <user> -P <pass> -C -f 65001 -i database\00_create_database.sql
     hoặc: python database\init_database.py --create-db --reset
   ===================================================================================== */

IF DB_ID(N'FastAPI') IS NULL
BEGIN
    -- Collation tiếng Việt, không phân biệt hoa/thường (CI) nhưng phân biệt dấu (AS):
    -- tìm "Quân" sẽ không khớp "Quan". Dữ liệu tiếng Việt lưu ở cột NVARCHAR nên luôn đúng.
    CREATE DATABASE FastAPI COLLATE Vietnamese_CI_AS;
END;
GO

/* -------------------------------------------------------------------------------------
   READ COMMITTED SNAPSHOT (RCSI): câu ĐỌC không phải chờ câu GHI (và ngược lại).
   - Không bật: lúc đang ghi 1 thông báo cho 2.000 người, các request đọc hộp thư / đếm chưa đọc
     của người dùng khác phải đứng chờ khóa.
   - Bật: câu đọc lấy bản đã commit gần nhất từ version store (tempdb), không chờ.
   - Câu claim outbox dùng UPDLOCK + READPAST vẫn hoạt động đúng.
   WITH ROLLBACK IMMEDIATE: ngắt các phiên đang kết nối để đổi cấu hình -> chạy khi ít người dùng.
   ------------------------------------------------------------------------------------- */
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'FastAPI' AND is_read_committed_snapshot_on = 0)
    ALTER DATABASE FastAPI SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
GO

/* -------------------------------------------------------------------------------------
   Recovery model
   - SIMPLE : log tự cắt sau mỗi checkpoint, không cần backup log. Phù hợp môi trường dev/test.
   - FULL   : khôi phục được tới từng thời điểm, nhưng PHẢI backup log định kỳ, nếu không file log
              phình to mãi. Dùng cho production cùng job "BACKUP LOG".
   ------------------------------------------------------------------------------------- */
ALTER DATABASE FastAPI SET RECOVERY SIMPLE;
GO

-- Tự tạo/cập nhật thống kê bất đồng bộ: truy vấn không phải chờ cập nhật thống kê khi bảng tăng nhanh.
ALTER DATABASE FastAPI SET AUTO_UPDATE_STATISTICS_ASYNC ON;
GO

SELECT name, collation_name, is_read_committed_snapshot_on, recovery_model_desc,
       is_auto_update_stats_async_on, compatibility_level
FROM sys.databases WHERE name = N'FastAPI';
GO
