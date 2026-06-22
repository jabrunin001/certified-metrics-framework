from __future__ import annotations
import hashlib
import json
from .models import GateResult, MetricCertificate
from .gates import governance_gate, reconciliation_gate, freshness_gate, variance_pct


def build_certificate(reg_row: dict, semantic_value: float | None,
                      reference_value: float | None, freshness_rows: list[dict],
                      semantic_metric_names: set[str], registry: list[dict],
                      as_of: str) -> MetricCertificate:
    gates = [
        governance_gate(registry, semantic_metric_names),
        freshness_gate(reg_row["metric"], reg_row["grain"], freshness_rows),
        reconciliation_gate(reg_row["metric"], reg_row["kind"],
                            semantic_value, reference_value),
    ]
    vp = (variance_pct(semantic_value, reference_value)
          if semantic_value is not None and reference_value is not None else None)
    cert = MetricCertificate(
        metric=reg_row["metric"], owner=reg_row["owner"],
        definition_source=reg_row["definition_file"], gates=gates,
        semantic_value=semantic_value, reference_value=reference_value,
        variance_pct=vp, as_of=as_of)
    payload = cert.dict()
    payload.pop("checksum", None)
    cert.checksum = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
    return cert


def render_registry_md(certs: list[MetricCertificate]) -> str:
    lines = ["# Certification Registry", "",
             "| Metric | Owner | Status | Variance | Checksum |",
             "| --- | --- | --- | --- | --- |"]
    for c in sorted(certs, key=lambda x: x.metric):
        status = "CERTIFIED" if c.certified else "FAILED"
        vp = "n/a" if c.variance_pct is None else f"{c.variance_pct:+.4f}%"
        lines.append(f"| {c.metric} | {c.owner} | {status} | {vp} | {c.checksum[:12]}… |")
    n_ok = sum(1 for c in certs if c.certified)
    lines += ["", f"**{n_ok}/{len(certs)} metrics certified.**"]
    return "\n".join(lines) + "\n"
