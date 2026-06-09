from __future__ import annotations

import json
import re
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from db.query_roles import activate_role, create_role, delete_role, get_all_roles, get_role_by_role_id, update_role
from schemas.schemas import RoleActivateRequest, RoleCreate, RoleUpdate
from utils.events import build_job_event, build_role_event
from services.notification_publisher import publish_job_event


# Regex kiểm tra role_id.
# Cho phép chữ, số, dấu gạch dưới, gạch ngang, dấu chấm và dấu hai chấm.
# Ví dụ hợp lệ:
# - admin
# - staff
# - job:create
# - system.admin
ROLE_ID_REGEX = r"^[a-zA-Z0-9_.:-]+$"


class RoleController:
    """
    Controller xử lý nghiệp vụ liên quan đến bảng roles.

    Controller có nhiệm vụ:
    - Validate dữ liệu đầu vào.
    - Kiểm tra quyền người gọi API.
    - Gọi các hàm query để thao tác database.
    - Chuyển lỗi nghiệp vụ thành HTTPException.

    Router/API không nên viết logic validate phức tạp.
    Router chỉ nên nhận request rồi gọi controller.
    """

    @staticmethod
    def _require_superuser(current_user: dict[str, Any]) -> None:
        """
        Kiểm tra người gọi API có phải superuser không.

        Vì role là dữ liệu phân quyền hệ thống,
        chỉ superuser hoặc admin hệ thống mới nên được tạo/sửa/xóa role.

        current_user thường lấy từ token JWT qua Depends(required_token_user).

        Ví dụ current_user:
        {
            "tenant_id": "tenant_demo",
            "user_id": "abc123",
            "username": "admin",
            "role_id": "admin",
            "is_superuser": True
        }
        """

        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

        if not current_user.get("is_superuser"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "Chỉ superuser mới được thao tác với role"},
            )

    @staticmethod
    def _validate_role_id(role_id: str | None) -> str | None:
        """
        Validate role_id.

        role_id có thể None nếu muốn server tự sinh.
        Nếu client truyền role_id thì cần kiểm tra:
        - Không rỗng.
        - Không vượt quá 64 ký tự.
        - Không chứa ký tự lạ.

        Hàm trả về role_id đã strip hoặc None.
        """

        if role_id is None:
            return None

        role_id = role_id.strip()

        if not role_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "role_id không được rỗng nếu đã truyền lên"},
            )

        if len(role_id) > 64:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "role_id không được vượt quá 64 ký tự"},
            )

        if not re.match(ROLE_ID_REGEX, role_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": (
                        "role_id chỉ được chứa chữ, số, dấu gạch dưới, "
                        "gạch ngang, dấu chấm hoặc dấu hai chấm"
                    )
                },
            )

        return role_id

    @staticmethod
    def _validate_role_name(role_name: str | None) -> str:
        """
        Validate role_name.

        role_name là tên hiển thị của role.
        Ví dụ:
        - Admin
        - Nhân viên
        - Quản lý job
        """

        if role_name is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "role_name là bắt buộc"},
            )

        role_name = role_name.strip()

        if not role_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "role_name không được rỗng"},
            )

        if len(role_name) > 64:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "role_name không được vượt quá 64 ký tự"},
            )

        return role_name

    @staticmethod
    def _validate_description(description: str | None) -> str | None:
        """
        Validate description.

        description có thể None.
        Nếu có truyền thì strip khoảng trắng và giới hạn độ dài.
        """

        if description is None:
            return None

        description = description.strip()

        if not description:
            return None

        if len(description) > 255:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "description không được vượt quá 255 ký tự"},
            )

        return description

    @staticmethod
    def _validate_permissions_json(permissions_json: str | None) -> str:
        """
        Validate permissions_json.

        Ví dụ hợp lệ:
        [
            "job:create",
            "job:read",
            "job:download"
        ]

        Hàm này kiểm tra:
        - Chuỗi có parse được JSON không.
        - JSON phải là list.
        - Mỗi phần tử trong list phải là string.
        - Sau đó convert lại thành chuỗi JSON chuẩn để lưu DB.
        """

        if permissions_json is None:
            return "[]"

        permissions_json = permissions_json.strip()

        if not permissions_json:
            return "[]"

        try:
            permissions = json.loads(permissions_json)

        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": (
                        "permissions_json không phải JSON hợp lệ. "
                        'Ví dụ đúng: ["job:create", "job:read"]'
                    )
                },
            )

        if not isinstance(permissions, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "permissions_json phải là một JSON array/list"},
            )

        for item in permissions:
            if not isinstance(item, str):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"message": "Mỗi permission trong permissions_json phải là chuỗi"},
                )

            if not item.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"message": "Permission không được là chuỗi rỗng"},
                )

        # Chuẩn hóa lại JSON trước khi lưu.
        # ensure_ascii=False để giữ tiếng Việt nếu có.
        return json.dumps(permissions, ensure_ascii=False)

    @staticmethod
    async def create_new_role(
        *,
        request: RoleCreate,
        db: AsyncSession,
        current_user: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Tạo role mới.

        Luồng xử lý:
        1. Kiểm tra người gọi API có phải superuser không.
        2. Validate role_id, role_name, description, permissions_json.
        3. Kiểm tra role_id đã tồn tại chưa nếu client tự truyền role_id.
        4. Gọi query create_role để insert DB.
        5. Trả response về router.
        """
        # Kiểm tra quyền superuser, nếu không phải sẽ throw 403 Forbidden.
        RoleController._require_superuser(current_user)

        # Lấy các trường thông tin và validate. Nếu không hợp lệ sẽ throw 400 Bad Request.
        role_id = RoleController._validate_role_id(request.role_id)
        role_name = RoleController._validate_role_name(request.role_name)
        description = RoleController._validate_description(request.description)
        permissions_json = RoleController._validate_permissions_json( request.permissions_json )

        # Nếu client tự truyền role_id, kiểm tra trước cho thông báo rõ ràng hơn.
        # Nếu không kiểm tra, DB vẫn có thể báo lỗi unique, nhưng message sẽ khó hiểu hơn.
        if role_id is not None:
            existing_role = await get_role_by_role_id(
                db,
                role_id=role_id,
                include_user_count=False,
            )

            if existing_role["success"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"message": f"role_id={role_id} đã tồn tại"},
                )

            if existing_role["success"] is False:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={"message": existing_role["message"]},
                )

        created = await create_role(
            db,
            role_id=role_id,
            role_name=role_name,
            description=description,
            permissions_json=permissions_json,
        )

        if not created["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": created["message"]},
            )
        
        # Lấy dữ liệu role vừa tạo.
        role_data = created["data"]

        # Tạo event role.created.
        event = build_job_event(
            event_type="role.created",
            tenant_id= "test",
            user_id= role_data["role_id"],
            job_id=role_data["role_name"],
            status= f"Role {role_data['role_name']} đã được tạo",
            progress="100",
            message= role_data.get("description"),
            filename= "",
        )

        # Ghi event vào global stream.
        # Lưu ý: notification không nên làm hỏng nghiệp vụ chính.
        # Nếu role đã tạo thành công nhưng ghi notification lỗi,
        # ta chỉ log lỗi, không rollback role.
        try:
            global_event_id = await publish_job_event(event=event, send_to_user= False, send_to_job= False, send_to_tenant= False, send_to_global= True)

            # Có thể gắn event_id vào response để debug nếu muốn.
            created["data"]["global_event_id"] = global_event_id

        except Exception as exc:
            print(f"Không thể ghi dữ liệu thông báo vào noti server: {exc}")
            # logger.warning(
            #     "Không thể ghi role.created event vào global stream",
            #     extra={
            #         "role_id": role_data.get("role_id"),
            #         "error_type": type(exc).__name__,
            #         "error_message": str(exc),
            #     },
            # )

        return created

    @staticmethod
    async def get_role_detail(
        *,
        role_id: str,
        db: AsyncSession,
        current_user: dict[str, Any],
        include_user_count: bool = False,
    ) -> dict[str, Any]:
        """
        Lấy chi tiết một role.

        Có thể cho user thường xem role hoặc chỉ cho superuser xem,
        tùy yêu cầu hệ thống.

        Ở đây mình cho phép user đã đăng nhập được xem,
        nhưng nếu bạn muốn bảo mật hơn thì gọi _require_superuser().
        """

        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

        role_id = RoleController._validate_role_id(role_id)

        result = await get_role_by_role_id(
            db,
            role_id=role_id,
            include_user_count=include_user_count,
        )

        if result["success"] is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": result["message"]},
            )

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )

        return result

    @staticmethod
    async def get_list_roles(
        *,
        db: AsyncSession,
        current_user: dict[str, Any],
        include_inactive: bool = True,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """
        Lấy danh sách role.

        include_inactive:
        - True: lấy cả role đang bị khóa.
        - False: chỉ lấy role đang active.

        limit:
        - Giới hạn số lượng bản ghi trả về.
        """

        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

        if limit < 1 or limit > 5000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "limit phải nằm trong khoảng 1 đến 5000"},
            )

        result = await get_all_roles(
            db,
            include_inactive=include_inactive,
            limit=limit,
        )

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )

        return result

    @staticmethod
    async def update_role_info(
        *,
        role_id: str,
        request: RoleUpdate,
        db: AsyncSession,
        current_user: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Cập nhật role.

        Chỉ superuser được cập nhật role.

        Các field có thể cập nhật:
        - role_name
        - description
        - permissions_json
        """

        RoleController._require_superuser(current_user)

        role_id = RoleController._validate_role_id(role_id)

        role_name = None
        description = None
        permissions_json = None

        if request.role_name is not None:
            role_name = RoleController._validate_role_name(request.role_name)

        if request.description is not None:
            description = RoleController._validate_description(request.description)

        if request.permissions_json is not None:
            permissions_json = RoleController._validate_permissions_json(
                request.permissions_json
            )

        # Nếu cả 3 field đều None nghĩa là client không gửi gì để cập nhật.
        if (
            role_name is None
            and description is None
            and permissions_json is None
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Không có dữ liệu nào để cập nhật role"},
            )

        result = await update_role(
            db,
            role_id=role_id,
            role_name=role_name,
            description=description,
            permissions_json=permissions_json,
        )

        if result["success"] is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": result["message"]},
            )

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )

        return result

    @staticmethod
    async def activate_role_info(
        *,
        role_id: str,
        request: RoleActivateRequest,
        db: AsyncSession,
        current_user: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Khóa hoặc mở khóa role.

        activate=True:
        - role được kích hoạt.

        activate=False:
        - role bị khóa.
        - Không nên gán role này cho user mới.
        """

        RoleController._require_superuser(current_user)

        role_id = RoleController._validate_role_id(role_id)

        result = await activate_role(
            db,
            role_id=role_id,
            activate=request.activate,
        )

        if result["success"] is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": result["message"]},
            )

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )

        return result

    @staticmethod
    async def delete_role_info(
        *,
        role_id: str,
        db: AsyncSession,
        current_user: dict[str, Any],
        hard_delete: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        """
        Xóa role.

        hard_delete=False:
        - Khóa mềm role bằng is_active=False.
        - Đây là cách an toàn.

        hard_delete=True:
        - Xóa thật role khỏi DB.

        force=True:
        - Cho phép xóa cứng dù đang có user_auth sử dụng role này.
        - Không khuyến nghị vì có thể làm user_auth.role_id bị mồ côi.

        Chỉ superuser được xóa role.
        """

        RoleController._require_superuser(current_user)

        role_id = RoleController._validate_role_id(role_id)

        result = await delete_role(
            db,
            role_id=role_id,
            hard_delete=hard_delete,
            force=force,
        )

        if result["success"] is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": result["message"]},
            )

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": result["message"],
                    "data": result.get("data"),
                },
            )

        return result