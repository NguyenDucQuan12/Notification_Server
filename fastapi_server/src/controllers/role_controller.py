"""
Controller xử lý nghiệp vụ liên quan đến bảng roles.
"""
from __future__ import annotations
import json
import re
from typing import Any
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from db.query_roles import activate_role, create_role, delete_role, get_all_roles, get_role_by_role_id, update_role
from schemas.schemas import RoleActivateRequest, RoleCreate, RoleUpdate
from schemas.noti_schema import NotificationAction, NotificationActionType, NotificationResource, NotificationSeverity
from services.notification_orchestrator import NotificationDispatchMode, send_custom_notification
from utils.constants import ROLE_ID_REGEX
from utils.utils import parse_permissions


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
        Vì role là dữ liệu phân quyền hệ thống, chỉ superuser hoặc admin hệ thống mới nên được tạo/sửa/xóa role.
        `current_user` là dữ liệu token JWT qua `Depends(required_token_user)`

        Ví dụ current_user:
        ```
        {
            "tenant_id": "tenant_demo",
            "user_id": "abc123",
            "user_name": "admin",
            "role_id": "admin",
            "is_superuser": True
        }
        ```
        """

        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

        if not current_user.get("is_superuser"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={"message": "Bạn không có quyền thực hiện hành động này. Chỉ superuser mới được phép."},
            )

    @staticmethod
    def build_role_changes(*, before_role: dict[str, Any], after_role: dict[str, Any]) -> dict[str, Any]:
        """
        So sánh dữ liệu role trước và sau cập nhật.  
        Kết quả ví dụ:

            {
                "role_name": {
                    "old": "Staff",
                    "new": "Warehouse Staff"
                },
                "description": {
                    "old": "Nhân viên",
                    "new": "Nhân viên kho"
                },
                "permissions": {
                    "added": ["warehouse:update"],
                    "removed": ["job:delete"]
                }
            }
        """
        # Tạo dict lưu các thay đổi
        changes: dict[str, Any] = {}

        # So sánh role_name
        old_role_name = before_role.get("role_name")
        new_role_name = after_role.get("role_name")

        if old_role_name != new_role_name:
            changes["role_name"] = {"old": old_role_name, "new": new_role_name}

        # So sánh description
        old_description = before_role.get("description")
        new_description = after_role.get("description")

        if old_description != new_description:
            changes["description"] = {"old": old_description, "new": new_description}

        # So sánh is_active
        old_is_active = before_role.get("is_active")
        new_is_active = after_role.get("is_active")

        if old_is_active != new_is_active:
            changes["is_active"] = {"old": old_is_active, "new": new_is_active}

        # So sánh permission
        old_permissions = set(parse_permissions(before_role.get("permissions_json")))
        new_permissions = set(parse_permissions(after_role.get("permissions_json")))

        added_permissions = sorted(new_permissions - old_permissions)
        removed_permissions = sorted(old_permissions - new_permissions)

        if added_permissions or removed_permissions:
            changes["permissions"] = {"added": added_permissions, "removed": removed_permissions}

        return changes

    @staticmethod
    def _validate_role_id(role_id: str | None) -> str | None:
        """
        `role_id` có thể `None` nếu muốn server tự sinh.  
        Nếu client truyền role_id thì cần kiểm tra:
        - Không rỗng.
        - Không vượt quá 64 ký tự.
        - Không chứa ký tự lạ.
        """
        if role_id is None:
            return None

        role_id = role_id.strip()

        if not role_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "role_id không được rỗng"},
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
        Validate role_name, là tên hiển thị của role  
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
        Validate description, là mô tả ngắn gọn về role  
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
                detail={"message": "Mô tả về role không được vượt quá 255 ký tự"},
            )

        return description

    @staticmethod
    def _validate_permissions_json(permissions_json: str | None) -> str:
        """
        Validate permissions_json, là danh sách các quyền của role  
        Ví dụ hợp lệ:  
        ```
        [
            "job:create",
            "job:read",
            "job:download"
        ]
        ```
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
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": (
                        "Danh sách các quyền (permissions_json) phải là một JSON hợp lệ. "
                        'Ví dụ đúng: ["job:create", "job:read"]'
                    )
                },
            ) from e

        if not isinstance(permissions, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Danh sách các quyền (permissions_json) phải là một JSON array/list"},
            )

        for item in permissions:
            if not isinstance(item, str):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"message": "Mỗi quyền trong permissions_json phải là chuỗi"},
                )

            if not item.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"message": "Quyền trong permissions_json không được là chuỗi rỗng"},
                )

        # Chuẩn hóa lại JSON trước khi lưu, ensure_ascii=False để giữ tiếng Việt nếu có.
        return json.dumps(permissions, ensure_ascii=False)

    @staticmethod
    async def create_new_role(*, request: RoleCreate, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        """
        Tạo role mới  
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
        if role_id is not None:
            existing_role = await get_role_by_role_id(db, role_id=role_id, include_user_count=False)

            if existing_role["success"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={"message": f"Mã role_id '{role_id}' đã tồn tại, vui lòng chọn role_id khác"},
                )

            if existing_role["success"] is False:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={"message": f"Lỗi khi kiểm tra role id: {existing_role["message"]}"},
                )

        # Tạo 1 role mới
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
                detail={"message": f" Lỗi khi tạo role mới: {created["message"]}"},
            )

        # ====================================================
        # TẠO NOTIFICATION
        # ====================================================

        # Lấy dữ liệu role vừa tạo.
        role_data = created["data"]

        permissions = json.loads(permissions_json)
        role_id_created = role_data["role_id"]
        role_name_created = role_data["role_name"]
        actor_name = current_user.get("user_name") or "Quản trị viên"

        # Người nhận: toàn bộ admin của tenant (stream cá nhân) + tenant dashboard + global.
        notification_result = await send_custom_notification(
            tenant_id=current_user["tenant_id"],
            current_user=current_user,
            notification_type="role.created",
            category="security",
            severity=NotificationSeverity.SUCCESS,
            title="Role mới đã được tạo",
            body=f"Role '{role_name_created}' đã được tạo bởi {actor_name}.",
            short_body=f"Role '{role_name_created}' vừa được tạo.",
            template_variables={
                "role_id": role_id_created,
                "role_name": role_name_created,
                "actor_user_id": str(current_user["user_id"]),
                "actor_username": actor_name,
            },
            target_user_ids=[],
            include_tenant_admins=False,
            send_to_tenant_dashboard=True,
            send_to_global=True,
            resource=NotificationResource(type="role", id=role_id_created, version=1),
            action=NotificationAction(
                type=NotificationActionType.NAVIGATE,
                target=f"/roles/{role_id_created}",
                label="Xem chi tiết",
                method="GET",
            ),
            data={
                "role_id": role_id_created,
                "role_name": role_name_created,
                "permissions": permissions,
            },
            deduplication_key=f"role:{current_user['tenant_id']}:{role_id_created}:created",
            ttl_seconds=7 * 24 * 60 * 60,
            causation_id=f"role_create:{role_id_created}",
            source_instance="role-controller",
            mode=NotificationDispatchMode.DIRECT,  # Muốn để worker gửi thì đổi: NotificationDispatchMode.WORKER
        )

        # Có thể trả metadata để debug. Không nên coi notification_failed là lỗi API tạo role.
        created["data"]["notification"] = notification_result
        return created

    @staticmethod
    async def get_role_detail(*, role_id: str, db: AsyncSession, current_user: dict[str, Any], include_user_count: bool = False) -> dict[str, Any]:
        """
        Lấy thông tin chi tiết một role (chỉ dành cho superuser hoặc admin hệ thống).
        """
        RoleController._require_superuser(current_user)

        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

        role_id = RoleController._validate_role_id(role_id)

        result = await get_role_by_role_id(db, role_id=role_id, include_user_count=include_user_count)

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
    async def get_list_roles(*, db: AsyncSession, current_user: dict[str, Any], include_inactive: bool = True, limit: int = 1000) -> dict[str, Any]:
        """
        Lấy danh sách role.

        include_inactive:
        - True: lấy cả role đang bị khóa.
        - False: chỉ lấy role đang active.

        limit:
        - Giới hạn số lượng bản ghi trả về.
        """
        RoleController._require_superuser(current_user)

        if not current_user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"message": "Chưa xác thực người dùng"},
            )

        if limit < 1 or limit > 5000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": "Giới hạn số lượng bản ghi phải nằm trong khoảng 1 đến 5000"},
            )

        result = await get_all_roles(db, include_inactive=include_inactive, limit=limit)

        if result["success"] is False:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": result["message"]},
            )

        return result

    @staticmethod
    async def update_role_info( *, role_id: str, request: RoleUpdate, db: AsyncSession, current_user: dict[str, Any],) -> dict[str, Any]:
        """
        Cập nhật role. Chỉ superuser được cập nhật role  
        """
        # Xác thưc người dùng
        RoleController._require_superuser(current_user)
        # Xác thực role id có hợp lệ không
        role_id = RoleController._validate_role_id(role_id)

        # Khai báo các trường được phép cập nhật
        role_name = None
        description = None
        permissions_json = None

        if request.role_name is not None:
            role_name = RoleController._validate_role_name(request.role_name)

        if request.description is not None:
            description = RoleController._validate_description(request.description)

        if request.permissions_json is not None:
            permissions_json = RoleController._validate_permissions_json(request.permissions_json)

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

        # Snapshot role TRƯỚC khi cập nhật, để thông báo biết được những gì đã thay đổi.
        before_result = await get_role_by_role_id(db, role_id=role_id, include_user_count=False)
        before_role: dict[str, Any] = dict(before_result.get("data") or {}) if before_result["success"] else {}

        # Tiến hành cập nhật dữ liệu cho role
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
        
        # ====================================================
        # TẠO NOTIFICATION
        # ====================================================

        # Snapshot role sau cập nhật, dùng để so sánh trước và sau cập nhật
        after_role = dict(result["data"])
        role_id_updated = str(after_role["role_id"])
        role_name_updated = str(after_role["role_name"])
        actor_name = current_user.get("username") or current_user.get("user_name") or "Quản trị viên"

        # so sánh trước và sau cập nhật để lấy ra những thay đổi
        changes = RoleController.build_role_changes(before_role=before_role, after_role=after_role)

        # Có thể lấy từ header X-Idempotency-Key để chống tạo thông báo trùng khi client retry.
        # Không dùng key cố định (role:x:updated) vì 2 lần cập nhật hợp lệ liên tiếp sẽ bị coi là trùng.
        idempotency_key: str | None = None

        notification_result = await send_custom_notification(
            tenant_id=current_user["tenant_id"],
            current_user=current_user,
            notification_type="role.updated",
            category="security",
            severity=NotificationSeverity.INFO,
            title="Role đã được cập nhật",
            body=f"Role '{role_name_updated}' đã được cập nhật bởi {actor_name}.",
            short_body=f"Role '{role_name_updated}' vừa được cập nhật.",
            template_variables={
                "role_id": role_id_updated,
                "role_name": role_name_updated,
                "actor_user_id": str(current_user["user_id"]),
                "actor_username": actor_name,
                "changes": changes,
            },
            target_user_ids=[],
            include_tenant_admins=False,
            send_to_tenant_dashboard=True,
            send_to_global=True,
            resource=NotificationResource(type="role", id=role_id_updated),
            action=NotificationAction(
                type=NotificationActionType.NAVIGATE,
                target=f"/roles/{role_id_updated}",
                label="Xem chi tiết",
                method="GET",
            ),
            data={
                "role_id": role_id_updated,
                "role_name": role_name_updated,
                "changes": changes,
                "updated_by": {"user_id": str(current_user["user_id"]), "username": actor_name},
            },
            deduplication_key=(
                f"role:{current_user['tenant_id']}:{role_id_updated}:updated:{idempotency_key}"
                if idempotency_key else None
            ),
            ttl_seconds=7 * 24 * 60 * 60,
            causation_id=f"role_update:{role_id_updated}",
            source_instance="role-controller",
            mode=NotificationDispatchMode.DIRECT,
        )
        # Cập nhật thêm thông tin notification vào response trả về
        result["data"]["notification"] = notification_result

        return result

    @staticmethod
    async def activate_role_info(*, role_id: str, request: RoleActivateRequest, db: AsyncSession, current_user: dict[str, Any]) -> dict[str, Any]:
        """
        Khóa hoặc mở khóa role.

        activate=True:
        - role được kích hoạt.

        activate=False:
        - role bị khóa.
        """
        RoleController._require_superuser(current_user)

        role_id = RoleController._validate_role_id(role_id)

        result = await activate_role(db, role_id=role_id, activate=request.activate)

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
    async def delete_role_info(*, role_id: str, db: AsyncSession, current_user: dict[str, Any], hard_delete: bool = False, force: bool = False) -> dict[str, Any]:
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

        result = await delete_role(db, role_id=role_id, hard_delete=hard_delete, force=force)

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

        # ====================================================
        # TẠO NOTIFICATION
        # ====================================================
        role_data = result.get("data") or {}
        role_name_deleted = str(role_data.get("role_name") or "")
        role_id_deleted = role_data["role_id"]
        actor_name = current_user.get("user_name") or "Quản trị viên"

        # Người nhận: toàn bộ admin của tenant (stream cá nhân) + tenant dashboard + global.
        notification_result = await send_custom_notification(
            tenant_id=current_user["tenant_id"],
            current_user=current_user,
            notification_type="role.deleted",
            category="security",
            severity=NotificationSeverity.SUCCESS,
            title="Role đã được xóa",
            body=f"Role '{role_name_deleted}' đã được xóa bởi {actor_name}.",
            short_body=f"Role '{role_name_deleted}' vừa được xóa.",
            template_variables={
                "role_id": role_id_deleted,
                "role_name": role_name_deleted,
                "actor_user_id": str(current_user["user_id"]),
                "actor_username": actor_name,
            },
            target_user_ids=[],
            include_tenant_admins=False,
            send_to_tenant_dashboard=True,
            send_to_global=True,
            resource=NotificationResource(type="role", id=role_id_deleted, version=1),
            action=NotificationAction(
                type=NotificationActionType.NAVIGATE,
                target=f"/roles/{role_id_deleted}",
                label="Xem chi tiết",
                method="GET",
            ),
            data={
                "role_id": role_id_deleted,
                "role_name": role_name_deleted
            },
            deduplication_key=f"role:{current_user['tenant_id']}:{role_id_deleted}:deleted",
            ttl_seconds=7 * 24 * 60 * 60,
            causation_id=f"role_delete:{role_id_deleted}",
            source_instance="role-controller",
            mode=NotificationDispatchMode.DIRECT
        )

        # Có thể trả metadata để debug. Không nên coi notification_failed là lỗi API tạo role.
        result["data"]["notification"] = notification_result

        return result
