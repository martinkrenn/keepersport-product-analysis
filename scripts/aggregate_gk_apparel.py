#!/usr/bin/env python3
"""
Phase 2 Step 1b: Aggregate GK Apparel invoices by parent SKU x period.

Input:  /Users/Martin/sap-cohort-analysis/d2c_invoices.csv
Output: 4 CSVs in /Users/Martin/sap-cohort-analysis/products/
  - gk_shirts_aggregated.csv     (Kat 1001 + 1003)
  - gk_pants_aggregated.csv      (Kat 1002)
  - gk_baselayers_aggregated.csv (Kat 1004 + 1005)
  - gk_sets_aggregated.csv       (Kat 1009)

Parent SKU = Produkt_Code with size stripped (everything before the final '/').
Period columns: full quarterly 2022-Q1 .. 2026-Q2, yearly 2022..2026.
"""
from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path

INVOICES = Path("/Users/Martin/sap-cohort-analysis/d2c_invoices.csv")
OUT_DIR = Path("/Users/Martin/sap-cohort-analysis/products")

# Category groups: label -> (output filename, set of Kategorie_Codes)
CATEGORY_GROUPS: dict[str, tuple[str, set[str]]] = {
    "Torwarttrikots + Trainingsoberteile": ("gk_shirts_aggregated.csv", {"1001", "1003"}),
    "Torwarthosen": ("gk_pants_aggregated.csv", {"1002"}),
    "Unterziehshirts + Unterziehhosen": ("gk_baselayers_aggregated.csv", {"1004", "1005"}),
    "Torwartsets": ("gk_sets_aggregated.csv", {"1009"}),
}

# Reverse lookup: Kategorie_Code -> group label
CODE_TO_GROUP: dict[str, str] = {}
for label, (_, codes) in CATEGORY_GROUPS.items():
    for c in codes:
        CODE_TO_GROUP[c] = label

# Classify parent SKU by pattern for resolution strategy
PAT_11TS = re.compile(r"^(\d{6,})_([A-Za-z0-9.]+)_([A-Za-z0-9]+)$")
PAT_DASH = re.compile(r"^(KS|RH|UA|NGS)([A-Z0-9]+)-([A-Z0-9]+)$")


def parse_num(s: str) -> float:
    if not s:
        return 0.0
    try:
        return float(s.replace(",", "."))
    except Exception:
        return 0.0


def parse_int(s: str) -> int:
    if not s:
        return 0
    try:
        return int(float(s.replace(",", ".")))
    except Exception:
        return 0


def parent_of(pc: str) -> str:
    """Strip trailing /SIZE to get the parent SKU."""
    if "/" not in pc:
        return pc
    return pc.rsplit("/", 1)[0]


def classify(parent: str) -> tuple[str, str | None]:
    """Return (resolution_hint, extracted_id_or_none)."""
    m = PAT_11TS.match(parent)
    if m:
        return ("11ts_direct", m.group(1))
    m = PAT_DASH.match(parent)
    if m:
        return ("dash_pattern", f"{m.group(1)}{m.group(2)}-{m.group(3)}")
    return ("legacy", None)


def quarter_of(date: str) -> str | None:
    """'2024-03-14' -> '2024-Q1'. Returns None if unparseable."""
    if not date or len(date) < 7 or date[4] != "-":
        return None
    try:
        y = date[:4]
        m = int(date[5:7])
        q = (m - 1) // 3 + 1
        return f"{y}-Q{q}"
    except Exception:
        return None


def year_of(q: str) -> str:
    return q.split("-")[0]


def main() -> None:
    # Accumulators per group label.
    # Key within each group: (marke_code, parent_sku)
    groups: dict[str, dict[tuple[str, str], dict]] = {
        label: defaultdict(
            lambda: {
                "marke": "",
                "kategorie_code": "",
                "kategorie": "",
                "product_name_top": "",
                "name_counts": defaultdict(int),
                "units_total": 0,
                "net_total": 0.0,
                "q": defaultdict(lambda: {"units": 0, "net": 0.0}),
            }
        )
        for label in CATEGORY_GROUPS
    }

    row_counts: dict[str, int] = defaultdict(int)

    # Single pass through the invoice file
    with INVOICES.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat_code = row["Kategorie_Code"]
            group_label = CODE_TO_GROUP.get(cat_code)
            if group_label is None:
                continue

            row_counts[group_label] += 1
            pc = row["Produkt_Code"]
            parent = parent_of(pc)
            mc = row["Marke_Code"]
            key = (mc, parent)

            units = parse_int(row["Rechnungsmenge"]) - parse_int(row["Gutschriftsmenge"])
            net = parse_num(row["Nettowert_Rechnungen"]) - parse_num(row["Gutschriftswert"])

            q = quarter_of(row["Rechnungsdatum"])
            rec = groups[group_label][key]
            rec["marke"] = row["Marke"]
            # First seen Kategorie_Code / Kategorie for this parent
            if not rec["kategorie_code"]:
                rec["kategorie_code"] = cat_code
                rec["kategorie"] = row["Kategorie"]
            rec["name_counts"][row["Produkt"]] += 1
            rec["units_total"] += units
            rec["net_total"] += net
            if q:
                rec["q"][q]["units"] += units
                rec["q"][q]["net"] += net

    # Period columns
    quarters = [f"{y}-Q{q}" for y in range(2022, 2027) for q in range(1, 5)]
    quarters = [q for q in quarters if q <= "2026-Q2"]
    years = [str(y) for y in range(2022, 2027)]

    header = [
        "Marke_Code",
        "Marke",
        "ParentSKU",
        "ProductName",
        "Kategorie_Code",
        "Kategorie",
        "Resolution_hint",
        "Extracted_ID",
        "Units_total",
        "Revenue_total",
    ]
    for y in years:
        header += [f"Units_{y}", f"Revenue_{y}"]
    for q in quarters:
        header += [f"Units_{q}", f"Revenue_{q}"]

    # Write one CSV per category group
    for label, (filename, _codes) in CATEGORY_GROUPS.items():
        agg = groups[label]
        rows = []
        for (mc, parent), rec in agg.items():
            hint, ext = classify(parent)
            pname = max(rec["name_counts"].items(), key=lambda x: x[1])[0] if rec["name_counts"] else ""

            # Year rollups from quarterly data
            year_u: dict[str, int] = defaultdict(int)
            year_n: dict[str, float] = defaultdict(float)
            for q, m in rec["q"].items():
                y = year_of(q)
                year_u[y] += m["units"]
                year_n[y] += m["net"]

            row_dict = {
                "Marke_Code": mc,
                "Marke": rec["marke"],
                "ParentSKU": parent,
                "ProductName": pname,
                "Kategorie_Code": rec["kategorie_code"],
                "Kategorie": rec["kategorie"],
                "Resolution_hint": hint,
                "Extracted_ID": ext or "",
                "Units_total": rec["units_total"],
                "Revenue_total": round(rec["net_total"], 2),
            }
            for y in years:
                row_dict[f"Units_{y}"] = year_u.get(y, 0)
                row_dict[f"Revenue_{y}"] = round(year_n.get(y, 0.0), 2)
            for q in quarters:
                row_dict[f"Units_{q}"] = rec["q"].get(q, {}).get("units", 0)
                row_dict[f"Revenue_{q}"] = round(rec["q"].get(q, {}).get("net", 0.0), 2)

            rows.append(row_dict)

        # Sort by revenue descending
        rows.sort(key=lambda r: -r["Revenue_total"])

        out_path = OUT_DIR / filename
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            w.writerows(rows)

        # --- Summary ---
        total_rev = sum(r["Revenue_total"] for r in rows)
        print(f"\n{'=' * 70}")
        print(f"{label}")
        print(f"  Invoice rows: {row_counts[label]:,}")
        print(f"  Parent SKUs:  {len(rows):,}")
        print(f"  Total revenue: EUR {total_rev:,.2f}")
        print(f"  Wrote: {out_path} ({len(rows):,} rows, {len(header)} cols)")

        # Resolution hint breakdown
        n_11ts = sum(1 for r in rows if r["Resolution_hint"] == "11ts_direct")
        n_dash = sum(1 for r in rows if r["Resolution_hint"] == "dash_pattern")
        n_leg = sum(1 for r in rows if r["Resolution_hint"] == "legacy")
        rev_11ts = sum(r["Revenue_total"] for r in rows if r["Resolution_hint"] == "11ts_direct")
        rev_dash = sum(r["Revenue_total"] for r in rows if r["Resolution_hint"] == "dash_pattern")
        rev_leg = sum(r["Revenue_total"] for r in rows if r["Resolution_hint"] == "legacy")

        print("  Resolution hints:")
        if total_rev:
            print(f"    11ts_direct : {n_11ts:>5} SKUs  EUR {rev_11ts:>12,.0f}  ({rev_11ts/total_rev*100:>5.1f}%)")
            print(f"    dash_pattern: {n_dash:>5} SKUs  EUR {rev_dash:>12,.0f}  ({rev_dash/total_rev*100:>5.1f}%)")
            print(f"    legacy      : {n_leg:>5} SKUs  EUR {rev_leg:>12,.0f}  ({rev_leg/total_rev*100:>5.1f}%)")
        else:
            print(f"    11ts_direct : {n_11ts:>5} SKUs")
            print(f"    dash_pattern: {n_dash:>5} SKUs")
            print(f"    legacy      : {n_leg:>5} SKUs")

        # Brand breakdown (top 10 by revenue)
        brand_rev: dict[str, float] = defaultdict(float)
        brand_units: dict[str, int] = defaultdict(int)
        for r in rows:
            brand_key = f"{r['Marke_Code']} ({r['Marke']})"
            brand_rev[brand_key] += r["Revenue_total"]
            brand_units[brand_key] += r["Units_total"]
        top_brands = sorted(brand_rev.items(), key=lambda x: -x[1])[:10]
        print("  Top brands by revenue:")
        for brand, rev in top_brands:
            pct = (rev / total_rev * 100) if total_rev else 0
            print(f"    {brand:<35} EUR {rev:>12,.0f}  {brand_units[brand]:>7,} units  ({pct:>5.1f}%)")


if __name__ == "__main__":
    main()
