"""Utility to send a concise Telegram notification for the SEO variations pipeline."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Optional


def _read_tail(summary_path: Optional[str], fallback: str) -> str:
    if not summary_path or not os.path.isfile(summary_path):
        return fallback
    try:
        with open(summary_path, "r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return fallback
    if not lines:
        return fallback
    tail = "".join(lines[-40:]).strip()
    return tail or fallback


def build_message(status: str, executed_at: str, variation_count: str, tail: Optional[str]) -> str:
    status_lower = status.lower()
    if status_lower == "success":
        headline = "Ejecucion completada correctamente."
    elif status_lower == "failure":
        headline = "Ejecucion con errores."
    else:
        headline = f"Ejecucion con estado: {status}"

    parts = [
        "🚀 Pipeline variaciones felinos",
        headline,
        f"Hora Bogota: {executed_at}",
        f"Variaciones registradas: {variation_count}",
    ]
    if tail and status_lower != "success":
        parts.extend(["", "Ultimas lineas:", tail])
    return "\n".join(parts)


def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    payload = json.dumps({"chat_id": chat_id, "text": text[:4000]}).encode()
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request) as response:
        print(f"Telegram respondio con estado {response.status}", file=sys.stdout)


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID deben estar definidos")

    status = os.environ.get("JOB_STATUS", "desconocido")
    variation_count = os.environ.get("VARIATION_COUNT") or "N/D"
    executed_at = os.environ.get("EXECUTED_AT") or "N/D"
    summary_path = os.environ.get("SUMMARY_FILE")

    tail = None
    if status.lower() != "success":
        tail = _read_tail(summary_path, "No se genero salida del pipeline.")

    message = build_message(status, executed_at, variation_count, tail)

    try:
        send_telegram_message(token, chat_id, message)
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Error enviando mensaje de Telegram: {exc}") from exc


if __name__ == "__main__":
    main()
