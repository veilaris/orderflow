"""
Webhook server: receives order notifications from RetailCRM and forwards
them to Telegram via Bot API.

IMPORTANT: This script must run on a server with a non-Russian IP address,
because Telegram is blocked in Russia and RetailCRM cannot reach it directly.

Usage:
    1. Copy .env.example to .env and fill in TG_BOT_TOKEN, TG_CHAT_ID, WEBHOOK_SECRET.
    2. pip install -r requirements.txt
    3. python tg_bot.py
    4. Expose the /webhook endpoint publicly (e.g. via ngrok or a VPS).
    5. In RetailCRM trigger settings, set the URL to:
       https://your-server/webhook
       and add header  X-Webhook-Secret: <your WEBHOOK_SECRET>

RetailCRM trigger fires only for orders with sumTotal > 50 000 ₸ (configured in CRM).
"""

import logging
import os
import sys

import requests
from dotenv import load_dotenv
from flask import Flask, Response, request

load_dotenv()

TG_BOT_TOKEN    = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID      = os.getenv("TG_CHAT_ID", "")
WEBHOOK_SECRET  = os.getenv("WEBHOOK_SECRET", "")
PORT            = int(os.getenv("PORT", 8000))

TG_API_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def validate_config() -> None:
    missing = [name for name, val in [
        ("TG_BOT_TOKEN",   TG_BOT_TOKEN),
        ("TG_CHAT_ID",     TG_CHAT_ID),
        ("WEBHOOK_SECRET", WEBHOOK_SECRET),
    ] if not val]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(text: str) -> None:
    resp = requests.post(
        TG_API_URL,
        json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
        timeout=10,
    )
    resp.raise_for_status()
    log.info("Telegram message sent: %s", text)


def fmt_sum(value) -> str:
    try:
        return f"{int(float(value)):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


def build_message(order: dict) -> str:
    number  = order.get("number") or order.get("id") or "—"
    total   = order.get("sumTotal") or order.get("totalSumm") or order.get("totalSum") or "—"
    return (
        f"🛒 <b>Новый крупный заказ</b>\n"
        f"Номер заказа: <b>#{number}</b>\n"
        f"Сумма: <b>{fmt_sum(total)} ₸</b>"
    )


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook() -> Response:
    # Verify secret token
    incoming_secret = (
        request.headers.get("X-Webhook-Secret")
        or request.args.get("secret")
    )
    if incoming_secret != WEBHOOK_SECRET:
        log.warning("Unauthorized webhook request (wrong or missing secret)")
        return Response("Unauthorized", status=401)

    # RetailCRM can send data as JSON body, urlencoded body, or query string
    payload = request.get_json(silent=True)
    if not payload:
        form = request.form.to_dict() or request.args.to_dict()
        if "order" in form:
            try:
                import json as _json
                payload = {"order": _json.loads(form["order"])}
            except Exception:
                payload = form
        else:
            payload = form

    log.info("Received payload keys: %s", list(payload.keys()) if payload else "empty")
    log.info("Received payload: %s", payload)

    # RetailCRM wraps the order under the "order" key
    order = payload.get("order") or payload

    if not order:
        log.warning("Empty or unrecognized payload")
        return Response("Bad Request", status=400)

    try:
        send_telegram(build_message(order))
    except requests.exceptions.RequestException as exc:
        log.error("Failed to send Telegram message: %s", exc)
        return Response("Telegram error", status=502)

    return Response("OK", status=200)


@app.route("/health", methods=["GET"])
def health() -> Response:
    return Response("OK", status=200)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    validate_config()
    log.info("Starting webhook server on port %d", PORT)
    app.run(host="0.0.0.0", port=PORT)
