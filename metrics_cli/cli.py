from __future__ import annotations
import glob
import hashlib
import json
import os
from pathlib import Path
import yaml
import typer
from .certificate import build_certificate, render_registry_md
from .mf_runner import (
    semantic_metric_total, dbt_parse_with_vars,
    read_reference_totals, read_freshness, read_registry,
)

app = typer.Typer(help="Certified Metrics Framework CLI")
DB_PATH = "cmf.duckdb"
AS_OF = "2026-03-31"


def _semantic_metric_names() -> set[str]:
    """Top-level metric names defined in the MetricFlow YAML (the governed set)."""
    names: set[str] = set()
    for path in glob.glob("models/semantic/*.yml"):
        doc = yaml.safe_load(Path(path).read_text()) or {}
        for m in (doc.get("metrics") or []):
            if isinstance(m, dict) and "name" in m:
                names.add(m["name"])
    return names


@app.command()
def certify(inject_break: bool = typer.Option(False, "--inject-break"),
            out: str = typer.Option("evidence", "--out")):
    """Run all gates per metric and emit certificates + registry. Exits 1 if any fail."""
    registry = read_registry(DB_PATH)
    references = read_reference_totals(DB_PATH)
    freshness = read_freshness(DB_PATH)
    semantic_names = _semantic_metric_names()
    os.makedirs(out, exist_ok=True)

    # Normalize inject_break: typer 0.12.x can deliver the string 'False'
    # instead of the bool False when no flag is supplied; bool('False') == True
    # which would silently activate the break path on every clean run.
    if isinstance(inject_break, str):
        inject_break = inject_break.lower() not in ("false", "0", "")

    # Compile the manifest once in the requested state; mf reads it from target/.
    if not dbt_parse_with_vars(inject_break):
        typer.echo("dbt parse failed; cannot compute semantic values", err=True)
        raise typer.Exit(code=2)

    certs = []
    for row in registry:
        metric = row["metric"]
        sem = semantic_metric_total(metric)
        cert = build_certificate(row, sem, references.get(metric), freshness,
                                 semantic_names, registry, AS_OF)
        certs.append(cert)
        Path(out, f"metric_certificate_{metric}.json").write_text(
            cert.json(indent=2))
        typer.echo(f"{'PASS' if cert.certified else 'FAIL'}  {metric}")

    Path(out, "certification_registry.md").write_text(render_registry_md(certs))
    failed = [c.metric for c in certs if not c.certified]
    if failed:
        typer.echo(f"\nUNCERTIFIED: {failed}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"\nAll {len(certs)} metrics certified.")


@app.command()
def pack(out: str = typer.Option("evidence", "--out")):
    """Write a checksum manifest over all evidence files (tamper-evident bundle)."""
    files = sorted(p for p in glob.glob(f"{out}/*") if not p.endswith("MANIFEST.sha256"))
    lines = []
    for p in files:
        digest = hashlib.sha256(Path(p).read_bytes()).hexdigest()
        lines.append(f"{digest}  {os.path.basename(p)}")
    Path(out, "MANIFEST.sha256").write_text("\n".join(lines) + "\n")
    typer.echo(f"Packed {len(files)} files into {out}/MANIFEST.sha256")


if __name__ == "__main__":
    app()
