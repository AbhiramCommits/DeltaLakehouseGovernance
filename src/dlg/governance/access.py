"""Role-to-grant access control.

``governance/roles.yml`` is the matrix; this module is the enforcement point.
``check_access(role, object, operation)`` returns an explicit allow/deny with a
machine-readable reason code, which is what the agent's SQL guard and the
service layer consult before anything is executed.

Hard rules enforced at load time and at check time:

* ``agent_service`` may only reference ``secure.*`` objects -- this is checked
  when the file is parsed (so a bad grant cannot even exist) *and* when a request
  is evaluated;
* ``agent_service`` is read-only: any write verb is denied regardless of grants.
"""

from __future__ import annotations

import fnmatch
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from dlg.config import Settings, get_settings

__all__ = [
    "AccessDecision",
    "RoleMatrix",
    "check_access",
    "load_roles",
    "matrix_table",
    "WRITE_OPERATIONS",
    "READ_OPERATIONS",
]

READ_OPERATIONS = ("SELECT",)
WRITE_OPERATIONS = ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER")
ALL_OPERATIONS = READ_OPERATIONS + WRITE_OPERATIONS

#: role -> the only schema prefix it may ever reference (None = unrestricted)
ROLE_SCOPE_PREFIXES: dict[str, str | None] = {"agent_service": "secure."}

#: roles that may never perform write operations
READ_ONLY_ROLES = ("agent_service", "analyst", "regional_analyst_emea")

REASON_ALLOW = "ALLOW_EXPLICIT_GRANT"
REASON_UNKNOWN_ROLE = "DENY_UNKNOWN_ROLE"
REASON_NO_GRANT = "DENY_NO_MATCHING_GRANT"
REASON_OPERATION = "DENY_OPERATION_NOT_GRANTED"
REASON_SCOPE = "DENY_ROLE_SCOPE_VIOLATION"
REASON_READ_ONLY = "DENY_ROLE_IS_READ_ONLY"


@dataclass(frozen=True)
class AccessDecision:
    role: str
    object: str
    operation: str
    allowed: bool
    reason_code: str
    reason: str

    def as_dict(self) -> dict:
        return asdict(self)

    def __bool__(self) -> bool:  # pragma: no cover - convenience
        return self.allowed


@dataclass
class Grant:
    object: str
    operations: list[str]

    def matches(self, target: str) -> bool:
        return fnmatch.fnmatch(target, self.object)


class RoleMatrix:
    def __init__(self, roles: dict[str, dict]) -> None:
        self.roles = roles

    @property
    def role_names(self) -> list[str]:
        return sorted(self.roles)

    def grants(self, role: str) -> list[Grant]:
        entry = self.roles.get(role)
        if entry is None:
            return []
        return [
            Grant(object=g["object"], operations=[op.upper() for op in g["operations"]])
            for g in entry.get("grants", [])
        ]

    def description(self, role: str) -> str:
        return str(self.roles.get(role, {}).get("description", "")).strip()

    # ------------------------------------------------------------------
    def check(self, role: str, obj: str, operation: str) -> AccessDecision:
        operation = operation.upper()
        obj = obj.strip().strip("`")

        if role not in self.roles:
            return AccessDecision(
                role, obj, operation, False, REASON_UNKNOWN_ROLE, f"unknown role {role!r}"
            )

        scope = ROLE_SCOPE_PREFIXES.get(role)
        if scope is not None and not obj.startswith(scope):
            return AccessDecision(
                role,
                obj,
                operation,
                False,
                REASON_SCOPE,
                f"{role} may only reference {scope}* objects (requested {obj})",
            )

        if role in READ_ONLY_ROLES and operation in WRITE_OPERATIONS:
            return AccessDecision(
                role,
                obj,
                operation,
                False,
                REASON_READ_ONLY,
                f"{role} is read-only; {operation} is not permitted",
            )

        matched_any = False
        for grant in self.grants(role):
            if grant.matches(obj):
                matched_any = True
                if operation in grant.operations:
                    return AccessDecision(
                        role,
                        obj,
                        operation,
                        True,
                        REASON_ALLOW,
                        f"granted by '{grant.object}' -> {operation}",
                    )

        if not matched_any:
            return AccessDecision(
                role, obj, operation, False, REASON_NO_GRANT, f"no grant covers {obj}"
            )
        return AccessDecision(
            role,
            obj,
            operation,
            False,
            REASON_OPERATION,
            f"a grant covers {obj} but not {operation}",
        )


def load_roles(path: str | Path | None = None, settings: Settings | None = None) -> RoleMatrix:
    settings = settings or get_settings()
    roles_path = Path(path or settings.roles_path)
    raw = yaml.safe_load(roles_path.read_text())
    roles = dict(raw.get("roles") or {})

    # --- load-time validation of the scope invariant -----------------------
    for role, scope in ROLE_SCOPE_PREFIXES.items():
        for grant in roles.get(role, {}).get("grants", []):
            obj = grant["object"]
            if scope and not obj.startswith(scope):
                raise ValueError(
                    f"roles.yml: {role} may only be granted {scope}* objects, found {obj!r}"
                )
            for operation in grant["operations"]:
                if operation.upper() in WRITE_OPERATIONS and role in READ_ONLY_ROLES:
                    raise ValueError(
                        f"roles.yml: {role} is read-only but was granted {operation} on {obj!r}"
                    )
    return RoleMatrix(roles)


def check_access(
    role: str,
    obj: str,
    operation: str,
    *,
    matrix: RoleMatrix | None = None,
    settings: Settings | None = None,
) -> AccessDecision:
    """Allow/deny ``role`` performing ``operation`` on ``object``."""
    matrix = matrix or load_roles(settings=settings)
    return matrix.check(role, obj, operation)


def matrix_table(
    objects: list[str],
    roles: list[str] | None = None,
    operation: str = "SELECT",
) -> list[list[str]]:
    """Rows for the printed access matrix: one row per role."""
    matrix = load_roles()
    role_names = roles or matrix.role_names
    rows: list[list[str]] = []
    for role in role_names:
        cells = []
        for obj in objects:
            decision = matrix.check(role, obj, operation)
            cells.append("allow" if decision.allowed else "deny")
        rows.append([role] + cells)
    return rows


def grant_totals(objects: list[str], roles: list[str] | None = None) -> dict[str, int]:
    """Allow/deny cell counts across the full (role, object, operation) grid."""
    matrix = load_roles()
    role_names = roles or matrix.role_names
    allow = deny = 0
    for role in role_names:
        for obj in objects:
            for operation in ALL_OPERATIONS:
                if matrix.check(role, obj, operation).allowed:
                    allow += 1
                else:
                    deny += 1
    return {"allow_cells": allow, "deny_cells": deny, "total_cells": allow + deny}
