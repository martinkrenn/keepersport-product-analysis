#!/usr/bin/env python3
"""
Phase 3 Step 2: Resolve remaining (non-11ts) apparel SKUs via KS backend MCP getProduct.

Reads 4 apparel aggregated CSVs, skips SKUs already in gk_apparel_11ts_attrs.csv.
For each remaining SKU, calls MCP getProduct and extracts apparel-specific attributes:
  color, sleeve-length, pants-cut, padding, collection, gender, fit, neckline, etc.

Output: gk_apparel_mcp_attrs.csv
"""
from __future__ import annotations

import csv
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/Users/Martin/sap-cohort-analysis/products")
OUT = ROOT / "gk_apparel_mcp_attrs.csv"
CACHE = ROOT / ".11ts_cache" / "mcp"

MCP_URL = "https://backend.keepersport.at/backend/mcp?token=07Zjh9C7lTwX5WfC0Kg4PwD1dZ5UHx6Y8eliWZuXxIIPXMkFOxtDF738dX6H1cEg"
MCP_AUTH = "Basic bWtyZW5uOnplcnNkeWtu"

APPAREL_FILES = [
    ROOT / "gk_shirts_aggregated.csv",
    ROOT / "gk_pants_aggregated.csv",
    ROOT / "gk_baselayers_aggregated.csv",
    ROOT / "gk_sets_aggregated.csv",
]

ALREADY_RESOLVED = ROOT / "gk_apparel_11ts_attrs.csv"


def mcp_init_session() -> str:
    """Initialize MCP session, return session ID."""
    payload = json.dumps({
        "jsonrpc": "2.0", "method": "initialize", "id": 0,
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "apparel-resolver", "version": "1.0"},
        }
    })
    proc = subprocess.run(
        ["curl", "-s", "-D", "/tmp/mcp_apparel_h.txt",
         "-X", "POST", MCP_URL,
         "-H", f"Authorization: {MCP_AUTH}",
         "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True, timeout=15,
    )
    with open("/tmp/mcp_apparel_h.txt") as f:
        for line in f:
            if line.lower().startswith("mcp-session-id:"):
                return line.split(":", 1)[1].strip()
    raise RuntimeError("No MCP session ID in initialize response")


def mcp_get_product(sku: str, session_id: str) -> dict | None:
    """Call MCP getProduct for a SKU. Returns product dict or None."""
    cache_file = CACHE / f"{sku.replace('/', '_')}.json"
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if data:
                return data
        except Exception:
            pass

    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "tools/call",
        "id": 1,
        "params": {
            "name": "getProduct",
            "arguments": {"sku": sku, "locale": "de"}
        }
    })

    try:
        proc = subprocess.run(
            ["curl", "-s", "-w", "\n%{http_code}",
             "-X", "POST", MCP_URL,
             "-H", f"Authorization: {MCP_AUTH}",
             "-H", "Content-Type: application/json",
             "-H", f"Mcp-Session-Id: {session_id}",
             "-d", payload],
            capture_output=True, text=True, timeout=15,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None

    body = proc.stdout.strip()
    lines = body.rsplit("\n", 1)
    if len(lines) != 2:
        return None
    resp_text, code = lines
    if code != "200":
        return None

    try:
        resp = json.loads(resp_text)
    except json.JSONDecodeError:
        return None

    result = resp.get("result", {})
    content_list = result.get("content", [])
    if not content_list:
        return None

    text_content = content_list[0].get("text", "")
    try:
        product = json.loads(text_content)
    except json.JSONDecodeError:
        return None

    if "error" in product:
        return None

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(product, ensure_ascii=False), encoding="utf-8")
    return product


def extract_attrs(product: dict) -> dict:
    """Extract flat attributes from MCP getProduct response, including apparel-specific."""
    attrs = product.get("attributes", {})

    def get_labels(key: str) -> str:
        items = attrs.get(key, [])
        if isinstance(items, list):
            return ", ".join(item.get("label", item.get("key", "")) for item in items)
        return ""

    def get_keys(key: str) -> str:
        items = attrs.get(key, [])
        if isinstance(items, list):
            return ", ".join(item.get("key", "") for item in items)
        return ""

    return {
        "MCP_Name": product.get("name", ""),
        "MCP_Type": product.get("type", ""),
        "MCP_Brand": get_labels("brand"),
        "MCP_Color_keys": get_keys("color"),
        "MCP_Color_labels": get_labels("color"),
        "MCP_Collection": get_labels("collection"),
        "MCP_Gender": get_labels("gender"),
        # Apparel-specific
        "MCP_Sleeve_length": get_keys("sleeve-length"),
        "MCP_Pants_cut": get_keys("pants-cut"),
        "MCP_Padding": get_keys("padding"),
        "MCP_Fit": get_keys("fit"),
        "MCP_Leg_type": get_keys("leg-type"),
        "MCP_Neckline": get_keys("neckline"),
        "MCP_Feature_textile": get_keys("feature-textile"),
        "MCP_Mesh_inset": get_keys("mesh-inset"),
        "MCP_Extra": get_keys("extra"),
    }


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)

    # Load already-resolved SKUs from 11ts
    resolved_skus = set()
    if ALREADY_RESOLVED.exists():
        with ALREADY_RESOLVED.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                resolved_skus.add(row["ParentSKU"])
    print(f"Already resolved via 11ts: {len(resolved_skus)} SKUs")

    # Collect remaining SKUs from all apparel aggregated CSVs
    work = []
    for agg_file in APPAREL_FILES:
        if not agg_file.exists():
            print(f"  SKIP {agg_file.name} (not found)")
            continue
        with agg_file.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sku = row["ParentSKU"]
                if sku not in resolved_skus:
                    work.append((
                        row["Marke"], sku, row["ProductName"],
                        float(row["Revenue_total"]),
                        row["Kategorie_Code"], row["Kategorie"],
                    ))

    # Deduplicate (a SKU shouldn't appear in multiple category files, but be safe)
    seen = set()
    deduped = []
    for item in work:
        if item[1] not in seen:
            seen.add(item[1])
            deduped.append(item)
    work = deduped

    work.sort(key=lambda x: -x[3])
    print(f"SKUs to resolve via MCP: {len(work)}")
    cached = sum(1 for _, s, *_ in work if (CACHE / f"{s.replace('/', '_')}.json").exists())
    print(f"  Already cached: {cached}")
    print(f"  To fetch: {len(work) - cached}")

    # Clear stale cache
    for cf in CACHE.glob("*.json"):
        try:
            d = json.loads(cf.read_text())
            if not d or "error" in str(d).lower()[:200]:
                cf.unlink()
        except Exception:
            cf.unlink()

    print("Initializing MCP session...")
    session = mcp_init_session()
    print(f"  Session: {session}")
    t0 = time.time()

    results: list[dict] = []
    n_ok = 0
    n_fail = 0

    for i, (marke, sku, name, rev, kat_code, kat_name) in enumerate(work):
        if i and i % 50 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(work) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(work)}]  ok={n_ok} fail={n_fail}  "
                  f"{rate:.1f}/s  ETA {eta/60:.1f} min")

        product = mcp_get_product(sku, session)
        row_base = {
            "Marke": marke,
            "ParentSKU": sku,
            "ProductName": name,
            "Revenue_total": rev,
            "Kategorie_Code": kat_code,
            "Kategorie": kat_name,
        }

        if not product:
            n_fail += 1
            row_base.update({k: "" for k in [
                "MCP_Name", "MCP_Type", "MCP_Brand",
                "MCP_Color_keys", "MCP_Color_labels", "MCP_Collection",
                "MCP_Gender", "MCP_Sleeve_length", "MCP_Pants_cut",
                "MCP_Padding", "MCP_Fit", "MCP_Leg_type", "MCP_Neckline",
                "MCP_Feature_textile", "MCP_Mesh_inset", "MCP_Extra",
            ]})
            row_base["MCP_resolved"] = "false"
            results.append(row_base)
            continue

        n_ok += 1
        flat = extract_attrs(product)
        row_base.update(flat)
        row_base["MCP_resolved"] = "true"
        results.append(row_base)

    elapsed = time.time() - t0
    print(f"\nDone. ok={n_ok}  fail={n_fail}  time={elapsed/60:.1f} min")

    fields = [
        "Marke", "ParentSKU", "ProductName", "Revenue_total",
        "Kategorie_Code", "Kategorie",
        "MCP_Name", "MCP_Type", "MCP_Brand",
        "MCP_Color_keys", "MCP_Color_labels", "MCP_Collection",
        "MCP_Gender", "MCP_Sleeve_length", "MCP_Pants_cut",
        "MCP_Padding", "MCP_Fit", "MCP_Leg_type", "MCP_Neckline",
        "MCP_Feature_textile", "MCP_Mesh_inset", "MCP_Extra",
        "MCP_resolved",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"Wrote {OUT} ({len(results)} rows, {n_ok} resolved, {n_fail} failed)")

    # Summary by category
    from collections import Counter
    ok_cat = Counter()
    fail_cat = Counter()
    for r in results:
        if r["MCP_resolved"] == "true":
            ok_cat[r["Kategorie"]] += 1
        else:
            fail_cat[r["Kategorie"]] += 1
    print("\nResolution by category:")
    for kat in sorted(set(list(ok_cat.keys()) + list(fail_cat.keys()))):
        print(f"  {kat:<35} ok={ok_cat[kat]:>4}  fail={fail_cat[kat]:>4}")

    # Attribute coverage
    print("\nAttribute coverage (non-empty, of resolved):")
    resolved = [r for r in results if r["MCP_resolved"] == "true"]
    for attr in ["MCP_Color_keys", "MCP_Sleeve_length", "MCP_Pants_cut",
                 "MCP_Padding", "MCP_Collection", "MCP_Gender", "MCP_Fit"]:
        n = sum(1 for r in resolved if r.get(attr, ""))
        print(f"  {attr:<25}: {n:>4} / {len(resolved)}")


if __name__ == "__main__":
    main()
