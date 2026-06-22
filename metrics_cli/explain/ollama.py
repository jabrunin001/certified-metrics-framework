from __future__ import annotations
import subprocess


def rewrite(explanation: str, *, model: str = "llama3.1:8b", runner=subprocess.run) -> str:
    prompt = (
        "Rewrite the following data-quality finding in two clear sentences for an "
        "analytics engineer. Do not change any numbers, metric names, or the root "
        f"cause.\n\nFinding: {explanation}"
    )
    try:
        result = runner(["ollama", "run", model, prompt],
                        capture_output=True, text=True, timeout=60)
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return explanation
    if getattr(result, "returncode", 1) != 0 or not result.stdout.strip():
        return explanation
    return result.stdout.strip()
