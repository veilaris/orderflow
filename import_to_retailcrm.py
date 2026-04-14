"""
Import orders from mock_orders.json to RetailCRM via API.

Usage:
    1. Copy .env.example to .env and fill in your credentials.
    2. pip install -r requirements.txt
    3. python import_to_retailcrm.py
"""

import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

RETAILCRM_URL = os.getenv("RETAILCRM_URL", "").rstrip("/")
RETAILCRM_API_KEY = os.getenv("RETAILCRM_API_KEY", "")
RETAILCRM_SITE = os.getenv("RETAILCRM_SITE", "")

ORDERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders")

# RetailCRM API v5 imposes a rate limit of 20 requests/second.
REQUEST_DELAY = 0.1  # seconds between requests


def validate_config() -> None:
    missing = [
        name
        for name, value in [
            ("RETAILCRM_URL", RETAILCRM_URL),
            ("RETAILCRM_API_KEY", RETAILCRM_API_KEY),
            ("RETAILCRM_SITE", RETAILCRM_SITE),
        ]
        if not value
    ]
    if missing:
        print(f"[ERROR] Missing required environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in the values.")
        sys.exit(1)


def load_orders_from_dir(directory: str) -> list[tuple[str, dict]]:
    """Return list of (filename, order) pairs from all JSON files in directory."""
    if not os.path.isdir(directory):
        print(f"[ERROR] Orders directory not found: {directory}")
        sys.exit(1)

    json_files = sorted(
        f for f in os.listdir(directory) if f.lower().endswith(".json")
    )
    if not json_files:
        print(f"[ERROR] No JSON files found in {directory}")
        sys.exit(1)

    all_orders: list[tuple[str, dict]] = []
    for filename in json_files:
        filepath = os.path.join(directory, filename)
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        # Support both a single order object and a list of orders per file.
        orders = data if isinstance(data, list) else [data]
        print(f"[INFO] {filename}: {len(orders)} order(s)")
        all_orders.extend((filename, order) for order in orders)

    print(f"[INFO] Total: {len(all_orders)} order(s) across {len(json_files)} file(s)")
    return all_orders


def build_order_payload(order: dict) -> dict:
    """Map fields from mock_orders.json to RetailCRM order format (API v5)."""
    payload: dict = {}

    # --- Customer info ---
    if order.get("firstName"):
        payload["firstName"] = order["firstName"]
    if order.get("lastName"):
        payload["lastName"] = order["lastName"]
    if order.get("phone"):
        payload["phone"] = order["phone"]
    if order.get("email"):
        payload["email"] = order["email"]

    # --- Order meta ---
    # orderType is intentionally skipped — RetailCRM substitutes the default
    # ("main") automatically for API orders. Add it back if you configure
    # custom order types in the directory (Справочники → Типы заказов).
    if order.get("orderMethod"):
        payload["orderMethod"] = order["orderMethod"]
    if order.get("status"):
        payload["status"] = order["status"]

    # --- Items ---
    items = order.get("items", [])
    if items:
        payload["items"] = [
            {
                "productName": item["productName"],
                "quantity": item["quantity"],
                "initialPrice": item["initialPrice"],
            }
            for item in items
        ]

    # --- Delivery ---
    delivery = order.get("delivery", {})
    address = delivery.get("address", {})
    if address:
        payload["delivery"] = {
            "address": {
                k: v for k, v in address.items() if v is not None
            }
        }

    # --- Custom fields (e.g. utm_source) ---
    custom_fields = order.get("customFields", {})
    if custom_fields:
        payload["customFields"] = custom_fields

    return payload


def create_order(payload: dict, index: int, source_file: str = "") -> bool:
    """Send a single order to RetailCRM. Returns True on success."""
    url = f"{RETAILCRM_URL}/api/v5/orders/create"
    data = {
        "apiKey": RETAILCRM_API_KEY,
        "site": RETAILCRM_SITE,
        "order": json.dumps(payload, ensure_ascii=False),
    }

    try:
        response = requests.post(url, data=data, timeout=30)
        result = response.json()
    except requests.exceptions.RequestException as exc:
        print(f"  [ERROR] Order #{index}: network error — {exc}")
        return False
    except json.JSONDecodeError:
        print(f"  [ERROR] Order #{index}: unexpected response — {response.text[:200]}")
        return False

    tag = f"#{index}" + (f" ({source_file})" if source_file else "")
    if response.status_code == 201 and result.get("success"):
        crm_id = result.get("id", "?")
        print(f"  [OK]    Order {tag}: created, RetailCRM id={crm_id}")
        return True

    errors = result.get("errors") or result.get("error") or result
    print(f"  [FAIL]  Order {tag}: HTTP {response.status_code} — {errors}")
    return False


def main() -> None:
    validate_config()
    entries = load_orders_from_dir(ORDERS_DIR)

    success_count = 0
    fail_count = 0

    print(f"\n[INFO] Sending orders to {RETAILCRM_URL} (site: {RETAILCRM_SITE})\n")

    for index, (source_file, order) in enumerate(entries, start=1):
        payload = build_order_payload(order)
        if create_order(payload, index, source_file):
            success_count += 1
        else:
            fail_count += 1
        time.sleep(REQUEST_DELAY)

    total = success_count + fail_count
    print(f"\n[DONE] {success_count}/{total} orders imported successfully.")
    if fail_count:
        print(f"       {fail_count} order(s) failed — review errors above.")
        sys.exit(2)


if __name__ == "__main__":
    main()
