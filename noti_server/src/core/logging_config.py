"""
Cấu hình logging cho toàn bộ Notification Server.

Tại sao không dùng print()?
- print() không có mức độ (INFO/WARNING/ERROR), không có thời gian, không lọc được.
- Hệ thống log tập trung (ELK, Loki, Datadog, CloudWatch...) cần log có cấu trúc.

Hai chế độ:
- Dạng chữ (dev): dễ đọc khi nhìn terminal.
    2026-11-08 10:00:00 | INFO | noti.ws | WebSocket connected | conn_id=ab12 user_id=7
- Dạng JSON (production): mỗi dòng là 1 JSON object, máy dễ phân tích/tìm kiếm.
    {"ts": "...", "level": "INFO", "logger": "noti.ws", "msg": "WebSocket connected", "conn_id": "ab12", ...}

Cách ghi log kèm dữ liệu ngữ cảnh:
    logger.info("WebSocket connected", extra={"conn_id": ..., "user_id": ...})

Bảo mật: uvicorn tự log đường dẫn WebSocket KÈM query string, tức là log luôn
`?ticket=...`. Dù ticket chỉ dùng 1 lần, ta vẫn che nó đi (RedactSecretsFilter)
vì nguyên tắc là KHÔNG BAO GIỜ ghi thông tin xác thực vào log.
"""

from __future__ import annotations

import json
import logging
import logging.config
import re
from datetime import datetime, timezone
from typing import Any

# Các thuộc tính mặc định của LogRecord. Thuộc tính nào KHÔNG nằm trong đây
# là dữ liệu mình truyền qua `extra=` và sẽ được in kèm.
_STANDARD_ATTRS = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys()
) | {"message", "asctime", "color_message", "taskName"}

# Che giá trị của ticket / token / access_token trong chuỗi log.
_SECRET_PATTERN = re.compile(r"(?i)\b(ticket|token|access_token)=([^&\s\"']+)")


def _redact(text: str) -> str:
    return _SECRET_PATTERN.sub(r"\1=***", text)


_PRIMITIVE_TYPES = (str, int, float, bool, type(None), list, dict, tuple)


def _extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    """
    Lấy các field truyền qua `extra=` của logger.

    Chỉ giữ giá trị kiểu cơ bản. Uvicorn tự gắn object nội bộ (vd websocket=<WebSocketProtocol ...>)
    vào log, in ra chỉ gây nhiễu và có thể rất dài.
    """
    return {
        k: v
        for k, v in record.__dict__.items()
        if k not in _STANDARD_ATTRS and isinstance(v, _PRIMITIVE_TYPES)
    }


class RedactSecretsFilter(logging.Filter):
    """
    Filter che thông tin nhạy cảm trước khi log được ghi ra.

    Gắn vào HANDLER (không phải logger) để mọi log, kể cả của uvicorn, đều đi qua.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = _redact(message)
        if redacted != message:
            # Thay message đã che và xóa args để formatter không format lại.
            record.msg = redacted
            record.args = None
        return True  # Luôn cho log đi tiếp, chỉ sửa nội dung.


class JsonFormatter(logging.Formatter):
    """Mỗi log là 1 dòng JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(_extra_fields(record))
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        # default=str: object lạ (datetime, UUID...) tự chuyển thành chuỗi, không làm crash log.
        return json.dumps(payload, ensure_ascii=False, default=str)


class TextFormatter(logging.Formatter):
    """Dạng chữ dễ đọc, nối thêm các field extra ở cuối dòng."""

    def __init__(self) -> None:
        super().__init__(fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                         datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = _extra_fields(record)
        if extras:
            base += " | " + " ".join(f"{k}={v}" for k, v in extras.items())
        return base


def setup_logging(*, level: str = "INFO", json_logs: bool = False) -> None:
    """
    Gọi 1 lần khi khởi động (trong main.py), TRƯỚC khi uvicorn chạy.

    Cấu hình luôn cả logger của uvicorn để mọi log có cùng định dạng.
    """

    formatter = "json" if json_logs else "text"

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {"redact": {"()": RedactSecretsFilter}},
            "formatters": {
                "json": {"()": JsonFormatter},
                "text": {"()": TextFormatter},
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": formatter,
                    "filters": ["redact"],
                    "stream": "ext://sys.stdout",  # Docker thu log từ stdout.
                },
            },
            "root": {"level": level, "handlers": ["console"]},
            "loggers": {
                # propagate=False + handler riêng => không bị in 2 lần.
                "uvicorn": {"level": level, "handlers": ["console"], "propagate": False},
                "uvicorn.error": {"level": level, "handlers": ["console"], "propagate": False},
                "uvicorn.access": {"level": level, "handlers": ["console"], "propagate": False},
                # Thư viện redis rất "nói nhiều" ở DEBUG, giữ ở WARNING.
                "redis": {"level": "WARNING"},
            },
        }
    )


# Uvicorn nhận cấu hình log qua tham số log_config. Trả None để uvicorn
# KHÔNG ghi đè cấu hình mình đã đặt bằng setup_logging().
UVICORN_LOG_CONFIG = None
