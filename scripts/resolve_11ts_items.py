#!/usr/bin/env python3
"""
Phase 2 Step 2: Resolve 11ts-pattern parent SKUs via /api/v4/ItemInfo/1/1/{itemId}.

- Reads gk_gloves_aggregated.csv, filters Resolution_hint=11ts_direct.
- For each unique Extracted_ID (ItemID), calls v4 ItemInfo.
- Caches XML in products/.11ts_cache/itemid/{ItemID}.xml.
- Parses: Producer, ProducerArticleNumber, Herstellermodell, Marketingperiode,
  LaunchDate, Color1, Color2, Color3, HerstellerFarbbezeichnung, CanoicalCategoryId.
- Writes products/gk_gloves_itemattrs.csv.

Network: HTTP Basic Auth with env vars KS_API_USER / KS_API_PASS, fallback defaults.
Uses curl subprocess (same pattern as Phase 1 — avoids SSL cert issues on macOS py3.9).
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path("/Users/Martin/sap-cohort-analysis/products")
AGG = ROOT / "gk_gloves_aggregated.csv"
OUT = ROOT / "gk_gloves_itemattrs.csv"
CACHE = ROOT / ".11ts_cache" / "itemid"

USER = os.environ.get("KS_API_USER", "mkrenn")
PASS = os.environ.get("KS_API_PASS", "zersdykn")
BASE = "https://api.keepersport.at"


def fetch_iteminfo(item_id: str) -> str | None:
    """Fetch ItemInfo XML for an ItemID with on-disk cache. Returns XML text or None."""
    cache_file = CACHE / f"{item_id}.xml"
    if cache_file.exists() and cache_file.stat().st_size > 200:
        return cache_file.read_text(encoding="utf-8")

    url = f"{BASE}/api/v4/ItemInfo/1/1/{item_id}"
    try:
        proc = subprocess.run(
            [
                "curl", "-s", "-w", "\n%{http_code}",
                "-u", f"{USER}:{PASS}",
                "-H", "HCurrentBuyerParty: 1",
                url,
            ],
            check=True, capture_output=True, text=True, timeout=30,
        )
    except subprocess.CalledProcessError as e:
        print(f"  curl failed for {item_id}: {e}", file=sys.stderr)
        return None
    except subprocess.TimeoutExpired:
        print(f"  timeout for {item_id}", file=sys.stderr)
        return None

    body = proc.stdout
    # last line is http_code
    lines = body.rsplit("\n", 1)
    if len(lines) != 2:
        return None
    xml, code = lines
    if code != "200":
        print(f"  http {code} for {item_id}", file=sys.stderr)
        return None
    if len(xml) < 200:
        return None

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(xml, encoding="utf-8")
    return xml


def parse_iteminfo(xml_text: str) -> dict | None:
    """Extract flat attribute dict from ItemInfo XML."""
    try:
        # strip default ns on attributes
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    items = root.find("Items")
    if items is None:
        return None
    item = items.find("Item")
    if item is None:
        return None

    def txt(path: str) -> str:
        el = item.find(path)
        return el.text.strip() if el is not None and el.text else ""

    def color(idx: int) -> str:
        el = item.find(f"Colors/Color{idx}")
        if el is None:
            return ""
        return (el.get("Value") or "").strip()

    return {
        "ItemID": txt("ItemID"),
        "Producer": txt("Producer"),
        "ProducerArticleNumber": txt("ProducerArticleNumber"),
        "Variant": txt("Variant"),
        "Herstellermodell": txt("USER_Herstellermodell"),
        "Marketingperiode": txt("USER_Marketingperiode"),
        "LaunchDate": txt("LaunchDate")[:10],
        "ArticleGroup": txt("ArticleGroup"),
        "CanonicalCategoryId": txt("CanoicalCategoryId"),
        "Color1": color(1),
        "Color2": color(2),
        "Color3": color(3),
        "HerstellerFarbbezeichnung": txt("USER_HerstellerFarbbezeichnung"),
        "Active": txt("Active"),
        "SapMaterialNumber": txt("SapMaterialNumber"),
    }


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)

    # Build work list: unique ItemIDs (dedup across parent SKUs)
    itemids: dict[str, list[tuple[str, str, str]]] = {}  # item_id -> [(marke, parent_sku, name), ...]
    with AGG.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["Resolution_hint"] != "11ts_direct":
                continue
            iid = row["Extracted_ID"]
            if not iid:
                continue
            itemids.setdefault(iid, []).append(
                (row["Marke"], row["ParentSKU"], row["ProductName"])
            )

    print(f"Unique ItemIDs to resolve: {len(itemids):,}")
    cached = sum(1 for iid in itemids if (CACHE / f"{iid}.xml").exists())
    print(f"  Already cached: {cached}")
    to_fetch = len(itemids) - cached
    print(f"  To fetch: {to_fetch}")

    resolved_rows: list[dict] = []
    unresolved: list[tuple[str, list]] = []

    t0 = time.time()
    n_ok = 0
    n_fail = 0
    for i, (iid, parent_list) in enumerate(sorted(itemids.items())):
        if i and i % 50 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(itemids) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(itemids)}]  ok={n_ok} fail={n_fail}  {rate:.1f}/s  ETA {eta/60:.1f} min")

        xml = fetch_iteminfo(iid)
        if not xml:
            n_fail += 1
            unresolved.append((iid, parent_list))
            continue
        attrs = parse_iteminfo(xml)
        if not attrs:
            n_fail += 1
            unresolved.append((iid, parent_list))
            continue

        # Emit one row per parent-SKU sharing this ItemID
        for marke, parent, name in parent_list:
            row = {"Marke": marke, "ParentSKU": parent, "ProductName": name}
            row.update(attrs)
            resolved_rows.append(row)
        n_ok += 1

    print(f"\nDone. ok={n_ok}  fail={n_fail}  time={(time.time()-t0)/60:.1f} min")

    # Write output
    fields = [
        "Marke", "ParentSKU", "ProductName",
        "ItemID", "Producer", "ProducerArticleNumber", "Variant",
        "Herstellermodell", "Marketingperiode", "LaunchDate",
        "ArticleGroup", "CanonicalCategoryId",
        "Color1", "Color2", "Color3", "HerstellerFarbbezeichnung",
        "Active", "SapMaterialNumber",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(resolved_rows)
    print(f"Wrote {OUT} ({len(resolved_rows)} parent-SKU rows)")

    if unresolved:
        unres_file = ROOT / "gk_gloves_11ts_unresolved.csv"
        with unres_file.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ItemID", "Marke", "ParentSKU", "ProductName"])
            for iid, parents in unresolved:
                for m, p, n in parents:
                    w.writerow([iid, m, p, n])
        print(f"Wrote {unres_file} ({sum(len(p) for _, p in unresolved)} SKU rows)")


if __name__ == "__main__":
    main()
