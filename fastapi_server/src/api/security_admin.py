from typing import List, Optional
from fastapi import APIRouter, Depends, Body, Query
from auth.oauth2 import required_token_user  
from schemas.schemas import UserAuth
from controllers.security_admin_controller import Security_Admin_Controller


router = APIRouter(
    prefix="/security/admin",
    tags=["Security Admin"]
)


@router.post("/ban_now", summary="Ban ngay 1 IP (tuỳ chọn TTL)", response_model=dict)
def ban_now(ip: str = Body(..., embed=True, description="IPv4/IPv6 cần BAN"),
            ttl: Optional[int] = Body(None, embed=True, description="TTL giây; bỏ trống = mặc định"),
            user_info: UserAuth = Depends(required_token_user)):
    """
    Đặt BAN ngay 1 IP:
    - ip: chuỗi IPv4/IPv6
    - ttl: số giây; nếu None dùng TTL.ban_seconds
    """

    return Security_Admin_Controller.ban_now(user_info = user_info, ip=ip, ttl=ttl)

@router.post("/unban", summary="Gỡ ban 1 IP", response_model=dict)
def unban(ip: str = Body(..., embed=True, description="IPv4/IPv6 cần gỡ"),
          user_info: UserAuth = Depends(required_token_user)):
    """
    Gỡ ban 1 IP:
    - Trả deleted=1 nếu xoá được key ban:ip:<ip>, 0 nếu không tồn tại
    """
    return Security_Admin_Controller.unban(user_info = user_info, ip=ip)

@router.post("/unban_list", summary="Gỡ ban nhiều IP", response_model=dict)
def unban_list(ips: List[str] = Body(..., embed=True, description="Danh sách IP"),
               user_info: UserAuth = Depends(required_token_user)):
    """
    Gỡ ban nhiều IP một lần.
    - Trả về {done: n, total: m, details:[...]}
    """
    return Security_Admin_Controller.unban_list(user_info = user_info, ips = ips)

@router.get("/ban_ttl", summary="Xem TTL còn lại của IP bị BAN", response_model=dict)
def get_ban_ttl(ip: str = Query(..., description="IPv4/IPv6"),
           user_info: UserAuth = Depends(required_token_user)):
    """
    Lấy TTL còn lại (giây) của IP đang bị BAN.
    - Nếu trả về -2: không có key
    - Nếu -1: có key nhưng không có TTL (không nên xảy ra vì ta luôn set TTL)
    """
    return Security_Admin_Controller.get_ban_ttl(user_info = user_info, ip = ip)

@router.get("/top_suspicious", summary="Top-N IP nghi vấn (5 phút gần nhất)", response_model=dict)
def top_suspicious(limit: int = Query(50, ge=1, le=1000),
                   user_info: UserAuth = Depends(required_token_user)):
    """
    Duyệt các khoá sus:ip:*:5min còn TTL, sắp theo score giảm dần, cắt top-N.
    Đây là điểm “nghi vấn” tích luỹ trong 5 phút (middleware tăng khi vượt rate/pattern xấu/UA rỗng).
    """
    return Security_Admin_Controller.get_top_suspicious(user_info = user_info, limit = limit)

@router.get("/current_bans", summary="Danh sách IP đang bị BAN + TTL", response_model=dict)
def get_current_bans(user_info: UserAuth = Depends(required_token_user)):
    """
    Liệt kê IP đang bị BAN (TTL > 0).
    """
    return Security_Admin_Controller.get_current_ban(user_info = user_info)
