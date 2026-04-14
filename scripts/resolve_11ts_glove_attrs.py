#!/usr/bin/env python3
"""
Fetch 11ts /attributes for all glove ItemIDs already in gk_gloves_itemattrs.csv.

The main ItemInfo (colors, model, etc.) is already resolved. This script adds
the structured /attributes data: Schnitt (cut), Material, Technologien, etc.

Output: gk_gloves_11ts_extra_attrs.csv
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
ITEMATTRS = ROOT / "gk_gloves_itemattrs.csv"
OUT = ROOT / "gk_gloves_11ts_extra_attrs.csv"
CACHE = ROOT / ".11ts_cache" / "itemattr"

USER = os.environ.get("KS_API_USER", "mkrenn")
PASS = os.environ.get("KS_API_PASS", "zersdykn")
BASE = "https://api.keepersport.at"

ATTR_MAP = {
    "22": "Schnitt",
    "37": "Produktgruppe",
    "38": "Produktart",
    "72": "Material_1",
    "73": "Material_1_pct",
    "74": "Material_2",
    "75": "Material_2_pct",
    "76": "Material_3",
    "77": "Material_3_pct",
    "116": "Technologien",
    "201": "Nachhaltigkeit",
}


def fetch_attrs_xml(item_id: str) -> str | None:
    cache_file = CACHE / f"{item_id}.xml"
    if cache_file.exists() and cache_file.stat().st_size > 50:
        return cache_file.read_text(encoding="utf-8")

    url = f"{BASE}/api/v4/ItemInfo/1/1/{item_id}/attributes"
    try:
        proc = subprocess.run(
            ["curl", "-s", "-w", "\n%{http_code}",
             "-u", f"{USER}:{PASS}",
             "-H", "HCurrentBuyerParty: 1", url],
            check=True, capture_output=True, text=True, timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None

    lines = proc.stdout.rsplit("\n", 1)
    if len(lines) != 2:
        return None
    xml, code = lines
    if code != "200" or len(xml) < 50:
        return None

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(xml, encoding="utf-8")
    return xml


def parse_attrs(xml_text: str) -> dict:
    result = {v: "" for v in ATTR_MAP.values()}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return result
    for attr in root.iter("ItemAttribute"):
        id_el = attr.find("Id")
        if id_el is not None and id_el.text and id_el.text.strip() in ATTR_MAP:
            val_el = attr.find("Value")
            val = val_el.text.strip() if val_el is not None and val_el.text else ""
            result[ATTR_MAP[id_el.text.strip()]] = val
    return result


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)

    # Get unique ItemIDs from existing glove itemattrs
    itemids: dict[str, list[str]] = {}  # ItemID -> [ParentSKU, ...]
    with ITEMATTRS.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            iid = row["ItemID"]
            if iid:
                itemids.setdefault(iid, []).append(row["ParentSKU"])

    unique = list(itemids.keys())
    print(f"Unique glove ItemIDs: {len(unique)}")
    cached = sum(1 for iid in unique if (CACHE / f"{iid}.xml").exists())
    print(f"  Cached: {cached}, To fetch: {len(unique) - cached}")

    results: list[dict] = []
    n_ok = 0
    n_fail = 0
    t0 = time.time()

    for i, iid in enumerate(sorted(unique)):
        if i and i % 100 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(unique) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(unique)}]  ok={n_ok} fail={n_fail}  "
                  f"{rate:.1f}/s  ETA {eta/60:.1f} min")

        xml = fetch_attrs_xml(iid)
        if not xml:
            n_fail += 1
            for sku in itemids[iid]:
                row = {"ItemID": iid, "ParentSKU": sku}
                row.update({v: "" for v in ATTR_MAP.values()})
                results.append(row)
            continue

        attrs = parse_attrs(xml)
        n_ok += 1
        for sku in itemids[iid]:
            row = {"ItemID": iid, "ParentSKU": sku}
            row.update(attrs)
            results.append(row)

    elapsed = time.time() - t0
    print(f"\nDone. ok={n_ok}  fail={n_fail}  time={elapsed/60:.1f} min")

    fields = ["ItemID", "ParentSKU"] + list(ATTR_MAP.values())
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"Wrote {OUT} ({len(results)} rows)")

    # Coverage
    from collections import Counter
    print("\nAttribute coverage (non-empty):")
    for attr in ["Schnitt", "Material_1", "Technologien", "Nachhaltigkeit"]:
        n = sum(1 for r in results if r[attr])
        print(f"  {attr:<20}: {n:>5} / {len(results)}")

    # Schnitt value distribution
    schnitt = Counter(r["Schnitt"] for r in results if r["Schnitt"])
    print(f"\nSchnitt (cut) values:")
    for val, cnt in schnitt.most_common():
        print(f"  {val:<25}: {cnt:>5}")


if __name__ == "__main__":
    main()
