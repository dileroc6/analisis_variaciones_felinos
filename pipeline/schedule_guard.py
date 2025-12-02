"""Determina si el pipeline debe ejecutarse segun una ventana externa de 28 dias.

La fecha ancla (UTC) se recibe mediante la variable de entorno
``ANCHOR_TIMESTAMP_UTC`` y se compara contra el instante actual.
Se escriben resultados en ``GITHUB_OUTPUT`` para que el workflow de
GitHub Actions pueda decidir si continuar.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone


def parse_anchor(raw: str) -> datetime:
    anchor = datetime.fromisoformat(raw)
    if anchor.tzinfo is None:
        return anchor.replace(tzinfo=timezone.utc)
    return anchor.astimezone(timezone.utc)


def compute_should_run(anchor: datetime, now: datetime) -> tuple[bool, int]:
    if now < anchor:
        return False, -1
    delta = now - anchor
    return (delta.days % 28 == 0), delta.days


def main() -> None:
    anchor_raw = os.environ.get("ANCHOR_TIMESTAMP_UTC", "2025-12-28T08:15:00+00:00")
    anchor = parse_anchor(anchor_raw)
    now = datetime.now(timezone.utc)
    should_run, days_elapsed = compute_should_run(anchor, now)

    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise RuntimeError("GITHUB_OUTPUT no esta definido en el entorno")

    with open(output_path, "a", encoding="utf-8") as handle:
        handle.write(f"should_run={'true' if should_run else 'false'}\n")
        handle.write(f"days_elapsed={days_elapsed}\n")
        handle.write(f"anchor_utc={anchor.isoformat()}\n")
        handle.write(f"now_utc={now.isoformat()}\n")

    print(f"Anchor UTC: {anchor.isoformat()}")
    print(f"Now UTC:    {now.isoformat()}")
    print(f"Days elapsed since anchor: {days_elapsed}")
    print(f"Should run today: {'yes' if should_run else 'no'}")


if __name__ == "__main__":
    main()
