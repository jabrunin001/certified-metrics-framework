from __future__ import annotations
from typing import Literal
from pydantic import BaseModel

class GateResult(BaseModel):
    name: str
    status: Literal["PASS", "FAIL", "SKIP"]
    detail: str = ""

class MetricCertificate(BaseModel):
    metric: str
    owner: str
    definition_source: str
    gates: list[GateResult]
    semantic_value: float | None = None
    reference_value: float | None = None
    variance_pct: float | None = None
    as_of: str
    checksum: str = ""

    @property
    def certified(self) -> bool:
        return all(g.status == "PASS" for g in self.gates)
