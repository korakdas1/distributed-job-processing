"""In-memory authenticated principal. Names are stable and non-secret."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Role(StrEnum):
    VIEWER = "viewer"
    OPERATOR = "operator"


@dataclass(frozen=True, slots=True)
class Principal:
    name: str
    role: Role | None

    @property
    def can_read(self) -> bool:
        return self.role in {Role.VIEWER, Role.OPERATOR} or self.role is None

    @property
    def can_write(self) -> bool:
        return self.role is Role.OPERATOR or self.role is None


ANONYMOUS = Principal(name="anonymous", role=None)
VIEWER_PRINCIPAL = Principal(name="viewer", role=Role.VIEWER)
OPERATOR_PRINCIPAL = Principal(name="operator", role=Role.OPERATOR)
