from __future__ import annotations


def classify(metric: str, kind: str, semantic_value: float, reference_value: float,
             refund_total: float | None = None) -> dict:
    diff = semantic_value - reference_value

    if refund_total is not None and reference_value != 0:
        if abs(diff - refund_total) <= max(1e-6, 1e-4 * abs(refund_total)):
            return {
                "cause": "refunds_not_subtracted",
                "confidence": "high",
                "explanation": (
                    f"{metric} semantic value exceeds the reference by "
                    f"{diff:,.2f}, which equals the period refund total "
                    f"({refund_total:,.2f}). The semantic measure is summing gross "
                    f"MRR instead of net (mrr_amount minus refunds). Inspect the "
                    f"net_revenue measure expression in _semantic_revenue.yml."
                ),
            }

    if reference_value != 0:
        ratio = semantic_value / reference_value
        nearest = round(ratio)
        if nearest >= 2 and abs(ratio - nearest) <= 0.02:
            return {
                "cause": "wrong_grain_or_missing_distinct",
                "confidence": "medium",
                "explanation": (
                    f"{metric} semantic value is about {nearest}x the reference "
                    f"({semantic_value:g} vs {reference_value:g}). A count metric "
                    f"inflated by an integer factor usually means a missing "
                    f"count(distinct ...) or a join that fans out the grain."
                ),
            }

    return {
        "cause": "unclassified",
        "confidence": "low",
        "explanation": (
            f"{metric} diverges from the reference ({semantic_value:g} vs "
            f"{reference_value:g}) without a recognized signature. Inspect the "
            f"measure definition and upstream joins manually."
        ),
    }
