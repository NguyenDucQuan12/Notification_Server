from __future__ import annotations

from datetime import datetime
from typing import Generic, TypeVar
from pydantic import BaseModel, ConfigDict, EmailStr, Field

from utils.constants import JobStatus

"""
File schemas.py

Mục đích:
- Định nghĩa dữ liệu client gửi lên API.
- Định nghĩa dữ liệu API trả về client.
- Giới hạn các trường nhạy cảm không được trả về, ví dụ:
    + password
    + password_hash
- Tạo tài liệu Swagger rõ ràng hơn trong FastAPI.
- Tách biệt giữa:
    + ORM model: dùng để làm việc với database.
    + Pydantic schema: dùng để nhận/trả dữ liệu qua API.

Lưu ý:
- Tên field trong schema nên thống nhất với tên field trong model SQLAlchemy.
- Ví dụ model dùng user_id thì schema cũng nên dùng user_id.
- Không nên dùng kiểu User_Name, Email, Password nếu model đang dùng snake_case.
"""


# ============================================================
# 1. RESPONSE CHUNG CHO TOÀN BỘ API
# ============================================================

T = TypeVar("T")


class APIResponse(BaseModel, Generic[T]):
    """
    Response chung cho toàn bộ API.

    Dùng generic T để data có thể là nhiều kiểu khác nhau:
    - UserDisplay
    - RoleDisplay
    - JobRecordDisplay
    - list[UserDisplay]
    - None

    Ví dụ:
    APIResponse[UserDisplay]
    APIResponse[list[JobRecordDisplay]]
    """

    success: bool | None = Field(
        default=False,
        description=(
            "Trạng thái xử lý. True là thành công, False là lỗi, "
            "None là không lỗi kỹ thuật nhưng không tìm thấy dữ liệu."
        ),
    )

    data: T | None = Field(
        default=None,
        description="Dữ liệu trả về cho client.",
    )

    message: str = Field(
        default="",
        description="Thông báo mô tả kết quả xử lý.",
    )


class MessageResponse(BaseModel):
    """
    Response đơn giản chỉ dùng để trả thông báo.

    Dùng cho các API như:
    - xóa dữ liệu
    - logout
    - kiểm tra trạng thái đơn giản
    """

    success: bool | None = False
    message: str = ""


# ============================================================
# 2. SCHEMA CHO BẢNG USERS
# ============================================================

class UserCreate(BaseModel):
    """
    Schema dữ liệu client gửi lên khi tạo hồ sơ người dùng.

    Bảng tương ứng: users

    Client cần cung cấp:
    - tenant_id: user thuộc tenant/khách hàng/nhóm nào
    - full_name: họ tên người dùng
    - email: email người dùng
    - phone: số điện thoại

    Không có user_id trong schema này vì user_id nên để server tự sinh.
    """
    # ... là bắt buộc phải có trường này khi tạo user.
    tenant_id: str = Field( ..., min_length=1, max_length=64, description="Mã tenant dùng để tách dữ liệu giữa các nhóm/khách hàng.", examples=["tenant_demo"], )
    full_name: str | None = Field( default=None, max_length=255, description="Họ tên người dùng.", examples=["Nguyễn Văn A"], )
    email: EmailStr | None = Field( default=None, description="Email người dùng.", examples=["user@example.com"], )
    phone: str | None = Field(default=None,max_length=32,description="Số điện thoại người dùng.",examples=["0987654321"],)


class UserUpdate(BaseModel):
    """
    Schema cập nhật hồ sơ người dùng.

    Chỉ field nào được truyền lên thì mới cập nhật.
    Nếu field = None thì service có thể hiểu là không cập nhật field đó.

    Không cho cập nhật các trường dưới đây qua schema vì tính nhạy cảm hoặc nghiệp vụ:
    - user_id
    - tenant_id
    - created_at
    """

    full_name: str | None = Field( default=None, max_length=255, description="Họ tên mới của người dùng.", )
    email: EmailStr | None = Field( default=None, description="Email mới của người dùng.", )
    phone: str | None = Field( default=None, max_length=32, description="Số điện thoại mới của người dùng.", )


class UserActivateRequest(BaseModel):
    """
    Schema khóa hoặc mở khóa hồ sơ người dùng.

    activate=True:
    - mở khóa user

    activate=False:
    - khóa user ở cấp hồ sơ/nghiệp vụ
    """

    activate: bool = Field( ..., description="True để kích hoạt user, False để khóa user.", )


class UserDisplay(BaseModel):
    """
    Schema trả về thông tin người dùng cho client.

    Không chứa dữ liệu nhạy cảm.

    Có thể dùng làm response_model cho API:
    - tạo user thành công
    - lấy chi tiết user
    - cập nhật user
    """
    # from_attributes=True giúp Pydantic tự động ánh xạ các trường từ model SQLAlchemy sang schema khi trả về response_model.
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: str
    tenant_id: str
    full_name: str | None = None
    email: EmailStr | None = None
    phone: str | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class UserCreateResponse(APIResponse[UserDisplay]):
    """
    Response khi tạo user thành công.

    data sẽ là UserDisplay.
    """

    pass


class UserListResponse(APIResponse[list[UserDisplay]]):
    """
    Response khi lấy danh sách user.

    data sẽ là list[UserDisplay].
    """

    pass


# ============================================================
# 3. SCHEMA CHO BẢNG ROLES
# ============================================================

class RoleCreate(BaseModel):
    """
    Schema tạo role mới.

    Bảng tương ứng: roles

    role_id:
    - Có thể để client truyền lên nếu bạn muốn tự đặt mã role.
    - Nếu không truyền, service có thể tự sinh uuid.

    permissions_json:
    - Lưu quyền dưới dạng chuỗi JSON.
    - Ví dụ: '["job:create", "job:read", "user:manage"]'
    """

    role_id: str | None = Field( default=None, max_length=64, description="Mã role. Nếu không truyền, server sẽ tự sinh.", examples=["admin"], )

    role_name: str = Field( ..., min_length=1, max_length=64, description="Tên role.", examples=["Admin"], )

    description: str | None = Field( default=None, max_length=255, description="Mô tả ngắn về role.", examples=["Quản trị hệ thống"], )

    permissions_json: str = Field( default="[]", description="Danh sách quyền ở dạng JSON string.", examples=['["job:create", "job:read", "user:manage"]'], )


class RoleUpdate(BaseModel):
    """
    Schema cập nhật role.

    Chỉ cập nhật các field được truyền lên khác None.
    """

    role_name: str | None = Field( default=None, min_length=1, max_length=64, description="Tên role mới.", )

    description: str | None = Field( default=None, max_length=255, description="Mô tả mới của role.", )

    permissions_json: str | None = Field( default=None, description="Danh sách quyền mới ở dạng JSON string.", )


class RoleActivateRequest(BaseModel):
    """
    Schema khóa hoặc mở khóa role.
    """

    activate: bool = Field( ..., description="True để kích hoạt role, False để khóa role.", )


class RoleDisplay(BaseModel):
    """
    Schema trả role về client.

    Không có dữ liệu nhạy cảm.
    """
    # from_attributes=True giúp Pydantic tự động ánh xạ các trường từ model SQLAlchemy sang schema khi trả về response_model.
    model_config = ConfigDict(from_attributes=True)

    id: int
    role_id: str
    role_name: str
    description: str | None = None
    permissions_json: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


class RoleWithUserCountDisplay(RoleDisplay):
    """
    Schema trả role kèm số lượng tài khoản đang dùng role.

    Dùng cho API quản trị.
    """

    user_count: int = 0


class RoleResponse(APIResponse[RoleDisplay]):
    """
    Response trả về một role.
    """

    pass


class RoleListResponse(APIResponse[list[RoleDisplay]]):
    """
    Response trả về danh sách role.
    """

    pass


# ============================================================
# 4. SCHEMA CHO BẢNG USER_AUTH
# ============================================================

class UserAuthCreate(BaseModel):
    """
    Schema tạo tài khoản đăng nhập cho user.

    Bảng tương ứng: user_auth

    Client gửi lên password dạng plain text.
    Service phải hash password trước khi lưu vào DB.

    Không bao giờ lưu thẳng password vào database.
    Không bao giờ trả password hoặc password_hash ra client.
    """

    tenant_id: str = Field( ..., min_length=1, max_length=64, description=( "Mã tenant. Vì bảng user_auth không có tenant_id trong model ban đầu, service sẽ dùng tenant_id để kiểm tra user thuộc đúng tenant." ), examples=["tenant_demo"], )

    user_id: str = Field( ..., min_length=1, max_length=64, description="Mã user cần tạo tài khoản đăng nhập.", examples=["0c8a9b2e8f724d5e9f3d9f8a12345678"], )

    username: str = Field( ..., min_length=3, max_length=128, description="Tên đăng nhập.", examples=["nguyenvana"], )

    password: str = Field( ..., min_length=6, max_length=128, description="Mật khẩu gốc do client gửi lên. Service phải hash trước khi lưu.", examples=["123456"], )

    role_id: str | None = Field( default=None, max_length=64, description="Mã role gán cho user. Có thể null nếu chưa gán quyền.", examples=["admin"], )

    is_superuser: bool = Field( default=False, description="True nếu đây là tài khoản superuser.", )


class UserAuthUpdate(BaseModel):
    """
    Schema cập nhật tài khoản đăng nhập.

    Không cập nhật password ở schema này.
    Đổi mật khẩu nên dùng schema ChangePasswordRequest.
    """

    username: str | None = Field( default=None, min_length=3, max_length=128, description="Username mới.", )

    role_id: str | None = Field( default=None, max_length=64, description="Role mới.", examples=["admin"], )

    is_superuser: bool | None = Field( default=None, description="Cập nhật trạng thái superuser.", examples=[True], )

    is_login_enabled: bool | None = Field( default=None, description="Cho phép hoặc khóa đăng nhập.", examples=[True], )


class LoginRequest(BaseModel):
    """
    Schema dùng cho API đăng nhập.

    Client gửi:
    - tenant_id
    - username
    - password

    Service sẽ:
    - tìm user_auth theo username
    - kiểm tra user thuộc tenant_id
    - kiểm tra password với password_hash trong database
    """

    tenant_id: str = Field( min_length=1, max_length=64, description="Mã tenant của người đăng nhập.", examples=["tenant_demo"], )

    username: str = Field( ..., min_length=3, max_length=128, description="Tên đăng nhập.", examples=["nguyenvana"], )

    password: str = Field( ..., min_length=1, max_length=128, description="Mật khẩu đăng nhập.", examples=["123456"], )


class ChangePasswordRequest(BaseModel):
    """
    Schema đổi mật khẩu.

    Client gửi mật khẩu mới.
    Service phải hash mật khẩu mới trước khi lưu vào UserAuth.password_hash.
    """

    new_password: str = Field( ..., min_length=6, max_length=128, description="Mật khẩu mới.", )


class LoginEnabledRequest(BaseModel):
    """
    Schema khóa hoặc mở khóa đăng nhập.

    enabled=True:
    - Cho phép đăng nhập.

    enabled=False:
    - Khóa đăng nhập.
    """

    enabled: bool = Field( ..., description="True để cho phép login, False để khóa login.", )


class UserAuthDisplay(BaseModel):
    """
    Schema trả thông tin auth_user về client.

    Không chứa:
    - password
    - password_hash

    Đây là schema an toàn để dùng trong response_model.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: str
    role_id: str | None = None
    username: str
    is_superuser: bool
    is_login_enabled: bool
    failed_login_count: int
    last_login_at: datetime | None = None
    password_changed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class UserAuthDetailDisplay(BaseModel):
    """
    Schema trả auth_user kèm thông tin user và role.

    Dùng cho API quản trị, ví dụ:
    - xem chi tiết tài khoản
    - xem user này có role gì
    """

    auth: UserAuthDisplay
    user: UserDisplay | None = None
    role: RoleDisplay | None = None


class UserAuthResponse(APIResponse[UserAuthDisplay]):
    """
    Response trả về một auth_user.
    """

    pass


class UserAuthDetailResponse(APIResponse[UserAuthDetailDisplay]):
    """
    Response trả về auth_user kèm user và role.
    """

    pass


# ============================================================
# 5. SCHEMA CHO ĐĂNG NHẬP VÀ TOKEN
# ============================================================

class TokenDisplay(BaseModel):
    """
    Schema trả token sau khi đăng nhập thành công.

    access_token:
    - token dùng để gọi các API cần xác thực.

    token_type:
    - thường là bearer.
    """

    access_token: str
    token_type: str = "bearer"


class LoginDisplay(BaseModel):
    """
    Schema trả về sau khi đăng nhập thành công.

    Không trả password_hash.
    """

    token: TokenDisplay
    user: UserDisplay
    auth: UserAuthDisplay
    role: RoleDisplay | None = None


class LoginResponse(APIResponse[LoginDisplay]):
    """
    Response của API login.
    """

    pass


# ============================================================
# 6. SCHEMA CHO BẢNG JOB_RECORDS
# ============================================================

class JobRecordCreate(BaseModel):
    """
    Schema tạo job_record.

    Bảng tương ứng: job_records

    Thực tế khi upload file bằng FastAPI, file thường được gửi bằng UploadFile,
    còn các thông tin như filename, upload_object_key có thể được service tạo ra
    sau khi lưu file.

    tenant_id và user_id thường nên lấy từ token đăng nhập.
    Tuy nhiên vẫn khai báo ở đây để bạn dễ test API.
    """

    tenant_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Mã tenant sở hữu job.",
        examples=["tenant_demo"],
    )

    user_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Mã user tạo job.",
        examples=["0c8a9b2e8f724d5e9f3d9f8a12345678"],
    )

    filename: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Tên file gốc đã được làm sạch.",
        examples=["data.xlsx"],
    )

    upload_object_key: str = Field(
        ...,
        min_length=1,
        description="Key/path của file upload trong storage.",
        examples=["uploads/tenant_demo/user_001/data.xlsx"],
    )

    trace_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Mã trace để liên kết log giữa API, worker và notification.",
        examples=["trace_abc_123"],
    )


class JobProgressUpdate(BaseModel):
    """
    Schema cập nhật tiến độ job.

    Thường do worker gọi.
    """

    status: JobStatus | None = Field( default=None, description="Trạng thái mới của job.", examples=["processing"], )

    progress: int | None = Field( default=None, ge=0, le=100, description="Tiến độ xử lý từ 0 đến 100.", examples=[50], )

    message: str | None = Field( default=None, max_length=255, description="Thông báo ngắn để hiển thị cho client.", examples=["Đang xử lý dữ liệu"], )


class JobSuccessRequest(BaseModel):
    """
    Schema đánh dấu job thành công.

    Worker gọi schema này sau khi xử lý xong file.
    """

    result_object_key: str = Field( ..., min_length=1, description="Key/path của file kết quả trong storage.", examples=["results/tenant_demo/user_001/result.xlsx"], )

    message: str = Field( default="Xử lý hoàn tất", max_length=255, description="Thông báo khi job thành công.", )


class JobFailedRequest(BaseModel):
    """
    Schema đánh dấu job thất bại.

    Worker gọi schema này khi xử lý lỗi.
    """

    error: str = Field( ..., min_length=1, max_length=255, description="Nội dung lỗi ngắn gọn.", examples=["File không đúng định dạng"], )

    message: str = Field( default="Xử lý thất bại", max_length=255, description="Thông báo lỗi hiển thị cho client.", )


class JobRecordDisplay(BaseModel):
    """
    Schema trả thông tin job_record về client.

    Có thể trả cho:
    - API tạo job thành công
    - API lấy trạng thái job
    - API lấy lịch sử job

    Không chứa thông tin quá nhạy cảm nếu storage key cần được bảo vệ.
    Nếu upload_object_key/result_object_key là private, bạn có thể tạo schema khác
    để ẩn hai trường này khỏi client thường.
    """

    model_config = ConfigDict(from_attributes=True)

    job_id: str
    tenant_id: str
    user_id: str
    filename: str
    status: JobStatus
    progress: int
    message: str
    attempts: int
    upload_object_key: str
    result_object_key: str | None = None
    error: str | None = None
    trace_id: str
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None


class JobRecordPublicDisplay(BaseModel):
    """
    Schema job_record rút gọn để trả cho người dùng thông thường.

    Khác với JobRecordDisplay:
    - Không trả upload_object_key
    - Không trả result_object_key
    - Không trả trace_id

    Dùng schema này nếu không muốn lộ đường dẫn/key file trong storage.
    """

    model_config = ConfigDict(from_attributes=True)

    job_id: str
    filename: str
    status: JobStatus
    progress: int
    message: str
    attempts: int
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None


class JobRecordWithUserDisplay(BaseModel):
    """
    Schema trả job kèm thông tin user tạo job.

    Dùng cho API quản trị.
    """

    job: JobRecordDisplay
    user: UserDisplay | None = None


class JobRecordResponse(APIResponse[JobRecordDisplay]):
    """
    Response trả về một job_record.
    """

    pass


class JobRecordPublicResponse(APIResponse[JobRecordPublicDisplay]):
    """
    Response trả về một job_record ở dạng public.
    """

    pass


class JobRecordListResponse(APIResponse[list[JobRecordDisplay]]):
    """
    Response trả về danh sách job_record.
    """

    pass


class JobRecordPublicListResponse(APIResponse[list[JobRecordPublicDisplay]]):
    """
    Response trả về danh sách job_record dạng public.
    """

    pass


# ============================================================
# 7. SCHEMA TỔNG HỢP USER + AUTH + ROLE + JOB
# ============================================================

class RegisterUserRequest(BaseModel):
    """
    Schema dùng cho API đăng ký/tạo người dùng mới.

    API này tạo đồng thời:
    - hồ sơ trong bảng users
    - tài khoản đăng nhập trong bảng user_auth

    Client gửi password dạng plain text.
    Controller sẽ hash password trước khi lưu vào DB.
    """

    tenant_id: str = Field( ..., min_length=1, max_length=64, description="Mã tenant/khách hàng/nhóm sở hữu user.", examples=["tenant_demo"], )

    full_name: str | None = Field( default=None, max_length=255, description="Họ tên người dùng.", examples=["Nguyễn Văn A"], )

    email: EmailStr | None = Field( default=None, description="Email người dùng.", examples=["user@example.com"], )

    phone: str | None = Field( default=None, max_length=32, description="Số điện thoại người dùng.", examples=["0987654321"], )

    username: str = Field( ..., min_length=3, max_length=128, description="Tên đăng nhập.", examples=["nguyenvana"], )

    password: str = Field( ..., min_length=6, max_length=128, description="Mật khẩu gốc. Controller sẽ hash trước khi lưu.", examples=["123456"], )

    role_id: str | None = Field( default=None, max_length=64, description="Mã role muốn gán cho user.", examples=["admin"], )

    is_superuser: bool = Field( default=False, description="True nếu tài khoản là superuser.", )


class RegisterUserDisplay(BaseModel):
    """
    Dữ liệu trả về sau khi tạo người dùng thành công.

    Không trả password.
    Không trả password_hash.
    """

    user: UserDisplay
    auth: UserAuthDisplay
    role: RoleDisplay | None = None

class RegisterUserResponse(APIResponse[RegisterUserDisplay]):
    """
    Response trả về sau khi tạo người dùng thành công.
    """

    pass

class UserDetailDisplay(BaseModel):
    """
    Schema trả thông tin chi tiết user.

    Dùng khi API join thủ công:
    users
    -> user_auth
    -> roles
    """

    user: UserDisplay
    auth: UserAuthDisplay | None = None
    role: RoleDisplay | None = None


class UserDetailResponse(APIResponse[UserDetailDisplay]):
    """
    Response trả về chi tiết user.
    """

    pass


class UserSummaryDisplay(BaseModel):
    """
    Schema trả thông tin tổng hợp user.

    Dùng khi muốn hiển thị:
    - hồ sơ user
    - thông tin auth
    - role
    - danh sách job
    """

    user: UserDisplay
    auth: UserAuthDisplay | None = None
    role: RoleDisplay | None = None
    job_count: int = 0
    jobs: list[JobRecordPublicDisplay] = []


class UserSummaryResponse(APIResponse[UserSummaryDisplay]):
    """
    Response trả về tổng hợp user.
    """

    pass


class BootstrapAdminRequest(BaseModel):
    """
    Dữ liệu tạo admin đầu tiên.

    API này chỉ dùng khi hệ thống chưa có admin.
    """

    bootstrap_secret: str = Field(..., description="Secret dùng để bootstrap hệ thống")

    tenant_id: str = Field(default="system", max_length=64)

    full_name: str | None = Field(default="System Admin", max_length=255)

    email: EmailStr | None = Field(default=None)

    phone: str | None = Field(default=None, max_length=32)

    username: str = Field(..., min_length=3, max_length=128)

    password: str = Field(..., min_length=6, max_length=128)