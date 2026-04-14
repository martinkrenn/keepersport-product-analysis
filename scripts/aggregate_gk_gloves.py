#!/usr/bin/env python3
"""
Phase 2 Step 1: Aggregate Kat 4000 (Torwarthandschuhe) invoices by parent SKU × period.

Input:  /Users/Martin/sap-cohort-analysis/d2c_invoices.csv
Output: /Users/Martin/sap-cohort-analysis/products/gk_gloves_aggregated.csv

Parent SKU = Produkt_Code with size stripped (everything before the final '/').
Period columns: full quarterly 2022-Q1 .. 2026-Q2, yearly 2022..2026, plus quarters_json.
"""
from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path

INVOICES = Path("/Users/Martin/sap-cohort-analysis/d2c_invoices.csv")
OUT = Path("/Users/Martin/sap-cohort-analysis/products/gk_gloves_aggregated.csv")

CAT_GK_GLOVES = "4000"

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
    # Cut at the last '/'
    return pc.rsplit("/", 1)[0]


def classify(parent: str) -> tuple[str, str | None]:
    """Return (resolution_hint, extracted_id_or_none)."""
    m = PAT_11TS.match(parent)
    if m:
        return ("11ts_direct", m.group(1))  # ItemID
    m = PAT_DASH.match(parent)
    if m:
        return ("dash_pattern", f"{m.group(1)}{m.group(2)}-{m.group(3)}")
    return ("legacy", None)


def quarter_of(date: str) -> str | None:
    """ '2024-03-14' -> '2024-Q1'. Returns None if unparseable."""
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
    # accumulator: parent_key -> {metrics}
    # parent_key = (marke_code, parent_sku)
    agg: dict[tuple[str, str], dict] = defaultdict(
        lambda: {
            "marke": "",
            "product_name_top": "",  # most frequent product name
            "name_counts": defaultdict(int),
            "units_total": 0,
            "net_total": 0.0,
            "q": defaultdict(lambda: {"units": 0, "net": 0.0}),
        }
    )

    row_count = 0
    with INVOICES.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Kategorie_Code"] != CAT_GK_GLOVES:
                continue
            row_count += 1
            pc = row["Produkt_Code"]
            parent = parent_of(pc)
            mc = row["Marke_Code"]
            key = (mc, parent)

            units = parse_int(row["Rechnungsmenge"]) - parse_int(row["Gutschriftsmenge"])
            net = parse_num(row["Nettowert_Rechnungen"]) - parse_num(row["Gutschriftswert"])

            q = quarter_of(row["Rechnungsdatum"])
            rec = agg[key]
            rec["marke"] = row["Marke"]
            rec["name_counts"][row["Produkt"]] += 1
            rec["units_total"] += units
            rec["net_total"] += net
            if q:
                rec["q"][q]["units"] += units
                rec["q"][q]["net"] += net

    print(f"Scanned {row_count:,} Kat 4000 invoice rows → {len(agg):,} parent SKUs")

    # Period columns
    quarters = [f"{y}-Q{q}" for y in range(2022, 2027) for q in range(1, 5)]
    # Data ends 2026-Q2, drop Q3/Q4 2026
    quarters = [q for q in quarters if q <= "2026-Q2"]
    years = [str(y) for y in range(2022, 2027)]

    header = [
        "Marke_Code",
        "Marke",
        "ParentSKU",
        "ProductName",
        "Resolution_hint",
        "Extracted_ID",
        "Units_total",
        "Revenue_total",
    ]
    for y in years:
        header += [f"Units_{y}", f"Revenue_{y}"]
    for q in quarters:
        header += [f"Units_{q}", f"Revenue_{q}"]
    rows = []
    for (mc, parent), rec in agg.items():
        hint, ext = classify(parent)
        # Most common product name
        pname = max(rec["name_counts"].items(), key=lambda x: x[1])[0] if rec["name_counts"] else ""

        # Year rollups
        year_u: dict[str, int] = defaultdict(int)
        year_n: dict[str, float] = defaultdict(float)
        for q, m in rec["q"].items():
            y = year_of(q)
            year_u[y] += m["units"]
            year_n[y] += m["net"]

        row = {
            "Marke_Code": mc,
            "Marke": rec["marke"],
            "ParentSKU": parent,
            "ProductName": pname,
            "Resolution_hint": hint,
            "Extracted_ID": ext or "",
            "Units_total": rec["units_total"],
            "Revenue_total": round(rec["net_total"], 2),
        }
        for y in years:
            row[f"Units_{y}"] = year_u.get(y, 0)
            row[f"Revenue_{y}"] = round(year_n.get(y, 0.0), 2)
        for q in quarters:
            row[f"Units_{q}"] = rec["q"].get(q, {}).get("units", 0)
            row[f"Revenue_{q}"] = round(rec["q"].get(q, {}).get("net", 0.0), 2)

        rows.append(row)

    # Sort by revenue descending
    rows.sort(key=lambda r: -r["Revenue_total"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {OUT} ({len(rows):,} rows, {len(header)} cols)")

    # Summary
    n_11ts = sum(1 for r in rows if r["Resolution_hint"] == "11ts_direct")
    n_dash = sum(1 for r in rows if r["Resolution_hint"] == "dash_pattern")
    n_leg = sum(1 for r in rows if r["Resolution_hint"] == "legacy")
    total_rev = sum(r["Revenue_total"] for r in rows)
    rev_11ts = sum(r["Revenue_total"] for r in rows if r["Resolution_hint"] == "11ts_direct")
    rev_dash = sum(r["Revenue_total"] for r in rows if r["Resolution_hint"] == "dash_pattern")
    rev_leg = sum(r["Revenue_total"] for r in rows if r["Resolution_hint"] == "legacy")

    print("\nResolution hint breakdown:")
    print(f"  11ts_direct : {n_11ts:>5} SKUs  EUR {rev_11ts:>14,.0f}  ({rev_11ts/total_rev*100:>5.1f}%)")
    print(f"  dash_pattern: {n_dash:>5} SKUs  EUR {rev_dash:>14,.0f}  ({rev_dash/total_rev*100:>5.1f}%)")
    print(f"  legacy      : {n_leg:>5} SKUs  EUR {rev_leg:>14,.0f}  ({rev_leg/total_rev*100:>5.1f}%)")


if __name__ == "__main__":
    main()
