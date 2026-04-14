#!/usr/bin/env python3
"""
Phase 3 Step 1: Resolve 11ts-pattern apparel SKUs via ItemInfo + /attributes endpoints.

Reads 4 apparel aggregated CSVs, filters Resolution_hint=11ts_direct.
For each unique ItemID, fetches:
  1. /api/v4/ItemInfo/1/1/{itemId}         — colors, producer, model, launch date
  2. /api/v4/ItemInfo/1/1/{itemId}/attributes — Ärmellänge, Textillänge, Passform, Material, etc.

Caches both XML responses on disk.
Output: gk_apparel_11ts_attrs.csv
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
OUT = ROOT / "gk_apparel_11ts_attrs.csv"
CACHE_ITEM = ROOT / ".11ts_cache" / "itemid"
CACHE_ATTR = ROOT / ".11ts_cache" / "itemattr"

USER = os.environ.get("KS_API_USER", "mkrenn")
PASS = os.environ.get("KS_API_PASS", "zersdykn")
BASE = "https://api.keepersport.at"

APPAREL_FILES = [
    ROOT / "gk_shirts_aggregated.csv",
    ROOT / "gk_pants_aggregated.csv",
    ROOT / "gk_baselayers_aggregated.csv",
    ROOT / "gk_sets_aggregated.csv",
]

# Attribute IDs we care about from /attributes endpoint
ATTR_MAP = {
    "12": "Produktgruppe",
    "13": "Produktart",
    "19": "Serie",
    "72": "Material_1",
    "73": "Material_1_pct",
    "74": "Material_2",
    "75": "Material_2_pct",
    "89": "Extras",
    "102": "Passform",
    "104": "Ausschnitt",
    "106": "Textillaenge",
    "107": "Aermellaenge",
    "108": "Mesh_Einsaetze",
    "116": "Technologien",
}


def fetch_xml(url: str, cache_file: Path, min_size: int = 200) -> str | None:
    """Fetch XML from URL with on-disk cache. Returns XML text or None."""
    if cache_file.exists() and cache_file.stat().st_size > min_size:
        return cache_file.read_text(encoding="utf-8")

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
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"  fetch failed: {e}", file=sys.stderr)
        return None

    body = proc.stdout
    lines = body.rsplit("\n", 1)
    if len(lines) != 2:
        return None
    xml, code = lines
    if code != "200":
        print(f"  http {code} for {url}", file=sys.stderr)
        return None
    if len(xml) < min_size:
        return None

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(xml, encoding="utf-8")
    return xml


def parse_iteminfo(xml_text: str) -> dict | None:
    """Extract flat attribute dict from main ItemInfo XML."""
    try:
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


def parse_attributes(xml_text: str) -> dict:
    """Extract structured attributes from /attributes XML."""
    result = {v: "" for v in ATTR_MAP.values()}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return result

    for attr in root.iter("ItemAttribute"):
        attr_id = ""
        id_el = attr.find("Id")
        if id_el is not None and id_el.text:
            attr_id = id_el.text.strip()

        if attr_id in ATTR_MAP:
            val_el = attr.find("Value")
            val = val_el.text.strip() if val_el is not None and val_el.text else ""
            result[ATTR_MAP[attr_id]] = val

    return result


def main() -> None:
    CACHE_ITEM.mkdir(parents=True, exist_ok=True)
    CACHE_ATTR.mkdir(parents=True, exist_ok=True)

    # Build work list from all apparel aggregated CSVs
    # itemid -> [(marke, parent_sku, product_name, kategorie_code, kategorie)]
    itemids: dict[str, list[tuple[str, str, str, str, str]]] = {}
    for agg_file in APPAREL_FILES:
        if not agg_file.exists():
            print(f"  SKIP {agg_file.name} (not found)")
            continue
        with agg_file.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["Resolution_hint"] != "11ts_direct":
                    continue
                iid = row["Extracted_ID"]
                if not iid:
                    continue
                itemids.setdefault(iid, []).append((
                    row["Marke"],
                    row["ParentSKU"],
                    row["ProductName"],
                    row["Kategorie_Code"],
                    row["Kategorie"],
                ))

    print(f"Unique ItemIDs to resolve: {len(itemids)}")
    cached_item = sum(1 for iid in itemids if (CACHE_ITEM / f"{iid}.xml").exists())
    cached_attr = sum(1 for iid in itemids if (CACHE_ATTR / f"{iid}.xml").exists())
    print(f"  ItemInfo cached: {cached_item}")
    print(f"  Attributes cached: {cached_attr}")
    to_fetch = len(itemids) - min(cached_item, cached_attr)
    print(f"  To fetch (at least one endpoint): {to_fetch}")

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
            print(f"  [{i}/{len(itemids)}]  ok={n_ok} fail={n_fail}  "
                  f"{rate:.1f}/s  ETA {eta/60:.1f} min")

        # Fetch main ItemInfo
        item_url = f"{BASE}/api/v4/ItemInfo/1/1/{iid}"
        item_cache = CACHE_ITEM / f"{iid}.xml"
        item_xml = fetch_xml(item_url, item_cache)
        if not item_xml:
            n_fail += 1
            unresolved.append((iid, parent_list))
            continue

        item_attrs = parse_iteminfo(item_xml)
        if not item_attrs:
            n_fail += 1
            unresolved.append((iid, parent_list))
            continue

        # Fetch /attributes
        attr_url = f"{BASE}/api/v4/ItemInfo/1/1/{iid}/attributes"
        attr_cache = CACHE_ATTR / f"{iid}.xml"
        attr_xml = fetch_xml(attr_url, attr_cache, min_size=50)
        extra_attrs = parse_attributes(attr_xml) if attr_xml else {v: "" for v in ATTR_MAP.values()}

        # Emit one row per parent SKU sharing this ItemID
        for marke, parent, name, kat_code, kat_name in parent_list:
            row = {
                "Marke": marke,
                "ParentSKU": parent,
                "ProductName": name,
                "Kategorie_Code": kat_code,
                "Kategorie": kat_name,
            }
            row.update(item_attrs)
            row.update(extra_attrs)
            resolved_rows.append(row)
        n_ok += 1

    elapsed = time.time() - t0
    print(f"\nDone. ok={n_ok}  fail={n_fail}  time={elapsed/60:.1f} min")

    # Write output
    fields = [
        "Marke", "ParentSKU", "ProductName", "Kategorie_Code", "Kategorie",
        # From ItemInfo
        "ItemID", "Producer", "ProducerArticleNumber", "Variant",
        "Herstellermodell", "Marketingperiode", "LaunchDate",
        "ArticleGroup", "CanonicalCategoryId",
        "Color1", "Color2", "Color3", "HerstellerFarbbezeichnung",
        "Active", "SapMaterialNumber",
        # From /attributes
        "Produktgruppe", "Produktart", "Serie",
        "Aermellaenge", "Textillaenge", "Passform", "Ausschnitt",
        "Material_1", "Material_1_pct", "Material_2", "Material_2_pct",
        "Mesh_Einsaetze", "Extras", "Technologien",
    ]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(resolved_rows)
    print(f"Wrote {OUT} ({len(resolved_rows)} parent-SKU rows)")

    if unresolved:
        unres_file = ROOT / "gk_apparel_11ts_unresolved.csv"
        with unres_file.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ItemID", "Marke", "ParentSKU", "ProductName", "Kategorie_Code"])
            for iid, parents in unresolved:
                for m, p, n, kc, _ in parents:
                    w.writerow([iid, m, p, n, kc])
        print(f"Wrote {unres_file} ({sum(len(p) for _, p in unresolved)} SKU rows)")

    # Summary by category
    from collections import Counter
    cat_ok = Counter()
    cat_total = Counter()
    for r in resolved_rows:
        cat_ok[r["Kategorie"]] += 1
    for _, parents in itemids.items():
        for _, _, _, _, kat in parents:
            cat_total[kat] += 1

    print("\nResolution by category:")
    for kat in sorted(cat_total):
        print(f"  {kat:<35} ok={cat_ok[kat]:>4} / {cat_total[kat]:>4}")

    # Attribute coverage
    print("\nAttribute coverage (non-empty):")
    for attr in ["Color1", "Color2", "Color3", "Aermellaenge", "Textillaenge",
                 "Passform", "Serie", "Material_1", "Produktart"]:
        n = sum(1 for r in resolved_rows if r.get(attr, ""))
        print(f"  {attr:<25}: {n:>4} / {len(resolved_rows)}")


if __name__ == "__main__":
    main()
