from dataclasses import dataclass
from typing import Dict, Any, Literal

Status = Literal["OK", "WARN", "FAIL"]

@dataclass
class CheckResult:
    name: str
    status: Status
    metrics: Dict[str, float]
    details: Dict[str, Any]
    raw_file: str | None = None
