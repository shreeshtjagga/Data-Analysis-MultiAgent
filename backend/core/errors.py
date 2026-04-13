"""Structured pipeline error helpers."""

from typing import Any, Optional


def make_pipeline_error(
    *,
    code: str,
    message: str,
    agent: str,
    error_type: str = "pipeline",
    details: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "type": error_type,
        "agent": agent,
        "message": message,
        "details": details or {},
    }


def add_pipeline_error(
    errors: list[dict[str, Any]],
    *,
    code: str,
    message: str,
    agent: str,
    error_type: str = "pipeline",
    details: Optional[dict[str, Any]] = None,
) -> None:
    errors.append(
        make_pipeline_error(
            code=code,
            message=message,
            agent=agent,
            error_type=error_type,
            details=details,
        )
    )
