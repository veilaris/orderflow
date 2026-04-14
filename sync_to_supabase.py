"""
Incremental sync: RetailCRM orders → Supabase.

- Fetches only orders created or updated since the last successful sync.
- Upserts orders by retailcrm_id; replaces all items for updated orders.
- Saves the sync checkpoint to .sync_state.json after each run.

Usage:
    1. Run schema.sql in Supabase SQL Editor.
    2. Copy .env.example to .env and fill in credentials.
    3. pip install -r requirements.txt
    4. python sync_to_supabase.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

DEBUG = "--debug" in sys.argv

load_dotenv()

RETAILCRM_URL     = os.getenv("RETAILCRM_URL", "").rstrip("/")
RETAILCRM_API_KEY = os.getenv("RETAILCRM_API_KEY", "")
RETAILCRM_SITE    = os.getenv("RETAILCRM_SITE", "")
SUPABASE_URL      = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY      = os.getenv("SUPABASE_SECRET_KEY", "")

SYNC_STATE_FILE = Path(__file__).parent / ".sync_state.json"
PAGE_LIMIT = 100  # RetailCRM max page size


# ---------------------------------------------------------------------------
# Config & state helpers
# ---------------------------------------------------------------------------

def validate_config() -> None:
    missing = [
        name for name, val in [
            ("RETAILCRM_URL",       RETAILCRM_URL),
            ("RETAILCRM_API_KEY",   RETAILCRM_API_KEY),
            ("RETAILCRM_SITE",      RETAILCRM_SITE),
            ("SUPABASE_URL",        SUPABASE_URL),
            ("SUPABASE_SECRET_KEY", SUPABASE_KEY),
        ] if not val
    ]
    if missing:
        print(f"[ERROR] Missing environment variables: {', '.join(missing)}")
        sys.exit(1)


def load_last_synced_at() -> str | None:
    """Return ISO timestamp of the last successful sync, or None."""
    if SYNC_STATE_FILE.exists():
        state = json.loads(SYNC_STATE_FILE.read_text(encoding="utf-8"))
        return state.get("last_synced_at")
    return None


def save_last_synced_at(ts: str) -> None:
    SYNC_STATE_FILE.write_text(
        json.dumps({"last_synced_at": ts}, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Supabase REST API client
# ---------------------------------------------------------------------------

def sb_headers() -> dict:
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
    }


def sb_url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


def sb_upsert(table: str, row: dict, on_conflict: str) -> dict:
    """Upsert a single row; return the resulting record."""
    resp = requests.post(
        sb_url(table),
        headers={
            **sb_headers(),
            "Prefer": f"resolution=merge-duplicates,return=representation",
        },
        params={"on_conflict": on_conflict},
        json=row,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()[0]


def sb_delete(table: str, column: str, value) -> None:
    """Delete rows where column = value."""
    resp = requests.delete(
        sb_url(table),
        headers=sb_headers(),
        params={column: f"eq.{value}"},
        timeout=30,
    )
    resp.raise_for_status()


def sb_insert_many(table: str, rows: list[dict]) -> None:
    """Bulk insert rows."""
    resp = requests.post(
        sb_url(table),
        headers=sb_headers(),
        json=rows,
        timeout=30,
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# RetailCRM: fetch orders
# ---------------------------------------------------------------------------

def fetch_orders(updated_at_from: str | None) -> list[dict]:
    """Fetch all orders from RetailCRM with pagination."""
    all_orders: list[dict] = []
    page = 1

    while True:
        params: dict = {
            "apiKey": RETAILCRM_API_KEY,
            "limit":  PAGE_LIMIT,
            "page":   page,
        }
        if updated_at_from:
            # RetailCRM API v5 expects "Y-m-d H:i:s", not ISO 8601
            dt = datetime.fromisoformat(updated_at_from)
            params["filter[updatedAtFrom]"] = dt.strftime("%Y-%m-%d %H:%M:%S")

        try:
            resp = requests.get(
                f"{RETAILCRM_URL}/api/v5/orders",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as exc:
            print(f"[ERROR] RetailCRM request failed: {exc}")
            sys.exit(1)

        if not data.get("success"):
            print(f"[ERROR] RetailCRM API error: {data.get('errorMsg') or data}")
            sys.exit(1)

        orders = data.get("orders", [])
        all_orders.extend(orders)

        pagination  = data.get("pagination", {})
        total_pages = pagination.get("totalPageCount", 1)
        print(f"  [CRM] Page {page}/{total_pages} — {len(orders)} order(s)")

        if DEBUG and all_orders:
            o = all_orders[0]
            print("\n[DEBUG] First order field check:")
            print(f"  total sum fields  : sumTotal={o.get('sumTotal')!r}  totalSumm={o.get('totalSumm')!r}")
            print(f"  customFields      : {o.get('customFields')!r}")
            first_item = (o.get("items") or [{}])[0]
            print(f"  first item        : productName={first_item.get('productName')!r}  offer.name={( first_item.get('offer') or {}).get('name')!r}")
            print()

        if page >= total_pages:
            break
        page += 1

    return all_orders


# ---------------------------------------------------------------------------
# Data mapping: RetailCRM order → Supabase rows
# ---------------------------------------------------------------------------

def map_order(o: dict) -> dict:
    delivery = o.get("delivery") or {}
    address  = delivery.get("address") or {}
    custom   = o.get("customFields") or {}

    # RetailCRM may return total sum under different keys depending on version
    total_sum = o.get("sumTotal") or o.get("totalSumm") or o.get("totalSum")

    # Custom fields are returned by their code as configured in RetailCRM.
    # If utm_source is empty, check --debug output for the actual key name.
    utm_source = custom.get("utm_source") or custom.get("utmSource")

    return {
        "retailcrm_id":     o["id"],
        "retailcrm_number": o.get("number"),
        "status":           o.get("status"),
        "order_method":     o.get("orderMethod"),
        "first_name":       o.get("firstName"),
        "last_name":        o.get("lastName"),
        "phone":            o.get("phone"),
        "email":            o.get("email"),
        "delivery_city":    address.get("city"),
        "delivery_address": address.get("text"),
        "utm_source":       utm_source,
        "total_sum":        total_sum,
        "created_at":       o.get("createdAt"),
        "updated_at":       o.get("updatedAt"),
        "synced_at":        datetime.now(timezone.utc).isoformat(),
    }


def map_items(order_id: int, o: dict) -> list[dict]:
    crm_number = o.get("number")
    return [
        {
            "order_id":          order_id,
            "retailcrm_number":  crm_number,
            # RetailCRM may return the name in offer.name or productName
            "product_name":      (item.get("offer") or {}).get("name") or item.get("productName"),
            "quantity":          item.get("quantity"),
            "price":             item.get("initialPrice"),
        }
        for item in (o.get("items") or [])
    ]


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------

def sync_order(o: dict) -> None:
    record   = sb_upsert("orders", map_order(o), on_conflict="retailcrm_id")
    order_id = record["id"]
    sb_delete("order_items", "order_id", order_id)
    items = map_items(order_id, o)
    if items:
        sb_insert_many("order_items", items)
        # Note: delete+insert is safe because retailcrm_item_id is stable —
        # RetailCRM never reassigns item IDs.


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    validate_config()

    last_synced_at = load_last_synced_at()
    sync_start     = datetime.now(timezone.utc).isoformat()

    if last_synced_at:
        print(f"[INFO] Incremental sync — changes since {last_synced_at}")
    else:
        print("[INFO] First run — full sync of all orders")

    print("[INFO] Fetching orders from RetailCRM...")
    orders = fetch_orders(updated_at_from=last_synced_at)

    if not orders:
        print("[INFO] No new or updated orders. Nothing to sync.")
        save_last_synced_at(sync_start)
        return

    print(f"[INFO] {len(orders)} order(s) to sync\n")

    success = 0
    failed  = 0

    for o in orders:
        crm_id = o.get("id")
        number = o.get("number", "?")
        try:
            sync_order(o)
            print(f"  [OK]  Order #{number} (retailcrm_id={crm_id})")
            success += 1
        except Exception as exc:
            print(f"  [FAIL] Order #{number} (retailcrm_id={crm_id}): {exc}")
            failed += 1

    save_last_synced_at(sync_start)

    total = success + failed
    print(f"\n[DONE] {success}/{total} orders synced successfully.")
    if failed:
        print(f"       {failed} order(s) failed — review errors above.")
        sys.exit(2)


if __name__ == "__main__":
    main()
