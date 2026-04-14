#!/usr/bin/env python3
"""
Phase 2 Step 3+4: Resolve remaining (non-11ts) SKUs via KS backend MCP getProduct.

Works for: rehab dash-pattern, NIKE/Puma/UA dash-pattern, AND all legacy SKUs.
MCP getProduct returns: name, color, brand, collection, cut, surface per SKU.

Input:  gk_gloves_aggregated.csv (rows NOT already in gk_gloves_itemattrs.csv)
Output: gk_gloves_mcp_attrs.csv
"""
from __future__ import annotations

import csv
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/Users/Martin/sap-cohort-analysis/products")
AGG = ROOT / "gk_gloves_aggregated.csv"
ALREADY_RESOLVED = ROOT / "gk_gloves_itemattrs.csv"
OUT = ROOT / "gk_gloves_mcp_attrs.csv"
CACHE = ROOT / ".11ts_cache" / "mcp"

MCP_URL = "https://backend.keepersport.at/backend/mcp?token=07Zjh9C7lTwX5WfC0Kg4PwD1dZ5UHx6Y8eliWZuXxIIPXMkFOxtDF738dX6H1cEg"
MCP_AUTH = "Basic bWtyZW5uOnplcnNkeWtu"


def mcp_init_session() -> str:
    """Initialize MCP session, return session ID."""
    payload = json.dumps({
        "jsonrpc": "2.0", "method": "initialize", "id": 0,
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "batch-resolver", "version": "1.0"},
        }
    })
    proc = subprocess.run(
        ["curl", "-s", "-D", "/tmp/mcp_headers.txt",
         "-X", "POST", MCP_URL,
         "-H", f"Authorization: {MCP_AUTH}",
         "-H", "Content-Type: application/json",
         "-d", payload],
        capture_output=True, text=True, timeout=15,
    )
    # Extract session ID from response headers
    with open("/tmp/mcp_headers.txt") as f:
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

    # Parse MCP JSON-RPC response
    result = resp.get("result", {})
    content_list = result.get("content", [])
    if not content_list:
        return None

    text_content = content_list[0].get("text", "")
    try:
        product = json.loads(text_content)
    except json.JSONDecodeError:
        return None

    # Cache
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(product, ensure_ascii=False), encoding="utf-8")
    return product


def extract_attrs(product: dict) -> dict:
    """Extract flat attributes from MCP getProduct response."""
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
        "MCP_Cut": get_labels("glove-cut"),
        "MCP_Surface": get_labels("surface"),
        "MCP_Features": get_labels("feature-equipment"),
        "MCP_Gender": get_labels("gender"),
    }


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)

    # Load already-resolved ItemIDs (from 11ts direct)
    resolved_skus = set()
    if ALREADY_RESOLVED.exists():
        with ALREADY_RESOLVED.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                resolved_skus.add(row["ParentSKU"])

    # Load all aggregated SKUs not yet resolved
    work = []
    with AGG.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sku = row["ParentSKU"]
            if sku not in resolved_skus:
                work.append((row["Marke"], sku, row["ProductName"], float(row["Revenue_total"])))

    # Sort by revenue descending (resolve high-value first)
    work.sort(key=lambda x: -x[3])
    print(f"SKUs to resolve via MCP: {len(work)}")
    cached = sum(1 for _, s, _, _ in work if (CACHE / f"{s.replace('/', '_')}.json").exists())
    print(f"  Already cached: {cached}")
    print(f"  To fetch: {len(work) - cached}")

    results: list[dict] = []
    n_ok = 0
    n_fail = 0

    # Clear stale cache files (empty/error)
    for cf in CACHE.glob("*.json"):
        try:
            d = json.loads(cf.read_text())
            if not d or "error" in str(d).lower():
                cf.unlink()
        except Exception:
            cf.unlink()

    print("Initializing MCP session...")
    session = mcp_init_session()
    print(f"  Session: {session}")
    t0 = time.time()

    for i, (marke, sku, name, rev) in enumerate(work):
        if i and i % 50 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(work) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(work)}]  ok={n_ok} fail={n_fail}  {rate:.1f}/s  ETA {eta/60:.1f} min")

        product = mcp_get_product(sku, session)
        if not product:
            n_fail += 1
            results.append({
                "Marke": marke,
                "ParentSKU": sku,
                "ProductName": name,
                "Revenue_total": rev,
                "MCP_Name": "",
                "MCP_Type": "",
                "MCP_Brand": "",
                "MCP_Color_keys": "",
                "MCP_Color_labels": "",
                "MCP_Collection": "",
                "MCP_Cut": "",
                "MCP_Surface": "",
                "MCP_Features": "",
                "MCP_Gender": "",
                "MCP_resolved": "false",
            })
            continue

        n_ok += 1
        flat = extract_attrs(product)
        flat["Marke"] = marke
        flat["ParentSKU"] = sku
        flat["ProductName"] = name
        flat["Revenue_total"] = rev
        flat["MCP_resolved"] = "true"
        results.append(flat)

    elapsed = time.time() - t0
    print(f"\nDone. ok={n_ok}  fail={n_fail}  time={elapsed/60:.1f} min")

    fields = [
        "Marke", "ParentSKU", "ProductName", "Revenue_total",
        "MCP_Name", "MCP_Type", "MCP_Brand",
        "MCP_Color_keys", "MCP_Color_labels",
        "MCP_Collection", "MCP_Cut", "MCP_Surface",
        "MCP_Features", "MCP_Gender", "MCP_resolved",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"Wrote {OUT} ({len(results)} rows, {n_ok} resolved, {n_fail} failed)")

    # Summary by brand
    from collections import Counter
    ok_brand = Counter()
    fail_brand = Counter()
    for r in results:
        if r["MCP_resolved"] == "true":
            ok_brand[r["Marke"]] += 1
        else:
            fail_brand[r["Marke"]] += 1
    print("\nResolution by brand:")
    all_brands = set(list(ok_brand.keys()) + list(fail_brand.keys()))
    for b in sorted(all_brands):
        print(f"  {b:<22} ok={ok_brand[b]:>4}  fail={fail_brand[b]:>4}")


if __name__ == "__main__":
    main()
