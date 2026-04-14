#!/usr/bin/env python3
"""
Phase 3 Step 3: Merge all data sources into 4 enriched apparel CSVs.

Sources:
1. gk_{shirts,pants,baselayers,sets}_aggregated.csv  — revenue × period per parent SKU
2. gk_apparel_11ts_attrs.csv    — 11ts ItemInfo + /attributes (colors, cut, material)
3. gk_apparel_mcp_attrs.csv     — MCP getProduct attrs (color, sleeve, pants-cut, padding)
4. ks_farbcodes_enriched.csv    — Phase 1 KS Farbcode → color mapping

Color model (same as gloves):
  Basisfarbe   = dominant base color (English lowercase)
  Highlight_1  = first accent color
  Highlight_2  = second accent color
  Color_confidence: high / medium / low

Apparel-specific columns:
  Aermellaenge  = sleeve length (Langarm/Kurzarm) — shirts only
  Textillaenge  = textile length (Lang/Kurz/3_4/Normal) — pants/baselayers
  Passform      = fit (eng/schmal/normal/weit)
  Padding       = padding zones (elbow/hip/knee) — baselayers
  Material      = material composition string
  Serie         = collection/series name
  Produktart    = product type (Torhüter - Trikots, Underwear - Hosen, etc.)
"""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

ROOT = Path("/Users/Martin/sap-cohort-analysis/products")

# ── Color normalization ───────────────────────────────────────────────────

COLOR_NORMALIZE = {
    "schwarz": "black", "weiss": "white", "weiß": "white",
    "blau": "blue", "rot": "red", "gelb": "yellow",
    "gruen": "green", "grün": "green", "grau": "grey",
    "orange": "orange", "lila": "purple", "rosa": "pink",
    "tuerkis": "turquoise", "türkis": "turquoise",
    "silber": "silver", "gold": "gold", "beige": "beige",
    "braun": "brown", "mehrfarbig": "multicolor",
    "lightpink": "pink", "anthrazit": "grey",
}


def norm(c: str) -> str:
    c = c.strip().lower()
    return COLOR_NORMALIZE.get(c, c)


# ── KS Farbcode → Basisfarbe (subset relevant for apparel) ───────────────

KS_FARBCODE_BASISFARBE = {
    "000": "white", "001": "white", "011": "black",
    "108": "black", "210": "black", "401": "blue",
    "452": "blue", "630": "white", "804": "white",
    "903": "black", "906": "black", "907": "white",
    "909": "black", "991": "black",
    "010": "black", "066": "white", "091": "white",
    "471": "black", "407": "blue", "416": "blue",
    "901": "black", "905": "black", "908": "black",
    "701": "white", "454": "blue", "563": "black",
    "633": "black", "406": "blue", "423": "blue",
    "425": "blue", "555": "black", "557": "black",
    "413": "blue", "110": "black", "405": "blue",
    "166": "white", "755": "red", "111": "black",
    "116": "white", "700": "red", "760": "red",
}


# ── Data loaders ──────────────────────────────────────────────────────────

def load_agg(path: Path) -> dict[str, dict]:
    rows = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ParentSKU"]] = row
    return rows


def load_11ts_attrs() -> dict[str, dict]:
    rows = {}
    path = ROOT / "gk_apparel_11ts_attrs.csv"
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ParentSKU"]] = row
    return rows


def load_mcp_attrs() -> dict[str, dict]:
    rows = {}
    path = ROOT / "gk_apparel_mcp_attrs.csv"
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ParentSKU"]] = row
    return rows


def load_ks_farbcode_map() -> dict[str, dict]:
    fc_map = {}
    path = ROOT / "ks_farbcodes_enriched.csv"
    if not path.exists():
        return fc_map
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            fc = row.get("Farbcode", "").strip()
            if fc:
                fc_map[fc.zfill(3)] = row
    return fc_map


# ── Normalize cut values ─────────────────────────────────────────────────

SLEEVE_NORMALIZE = {
    "langarm": "Langarm", "kurzarm": "Kurzarm",
    "long-sleeve": "Langarm", "short-sleeve": "Kurzarm",
}

PANTS_NORMALIZE = {
    "lang": "Lang", "kurz": "Kurz", "3/4": "3_4", "normal": "Normal",
    "long": "Lang", "short": "Kurz", "three-quarter": "3_4",
}

PASSFORM_NORMALIZE = {
    "eng": "eng", "schmal": "schmal", "normal": "normal",
    "weit": "weit", "tight": "eng", "slim": "schmal",
    "regular": "normal", "loose": "weit",
}


def norm_sleeve(val: str) -> str:
    return SLEEVE_NORMALIZE.get(val.strip().lower(), val.strip())


def norm_pants(val: str) -> str:
    return PANTS_NORMALIZE.get(val.strip().lower(), val.strip())


def norm_passform(val: str) -> str:
    return PASSFORM_NORMALIZE.get(val.strip().lower(), val.strip())


# ── Resolution logic ─────────────────────────────────────────────────────

def resolve_apparel(
    sku: str,
    marke: str,
    attrs_11ts: dict | None,
    attrs_mcp: dict | None,
    fc_map: dict,
) -> dict:
    """Resolve color + apparel attributes from all sources."""
    result = {
        "Basisfarbe": "",
        "Highlight_1": "",
        "Highlight_2": "",
        "Farbbezeichnung_Hersteller": "",
        "Aermellaenge": "",
        "Textillaenge": "",
        "Passform": "",
        "Padding": "",
        "Material": "",
        "Serie": "",
        "Produktart": "",
        "Color_confidence": "low",
        "Resolution_source": "unresolved",
    }

    # ── Priority 1: 11ts (highest confidence for competitors) ──
    if attrs_11ts:
        result["Basisfarbe"] = norm(attrs_11ts.get("Color1", ""))
        result["Highlight_1"] = norm(attrs_11ts.get("Color2", ""))
        result["Highlight_2"] = norm(attrs_11ts.get("Color3", ""))
        result["Farbbezeichnung_Hersteller"] = attrs_11ts.get("HerstellerFarbbezeichnung", "")
        result["Color_confidence"] = "high"
        result["Resolution_source"] = "11ts"

        # Apparel attrs from 11ts /attributes
        arm = attrs_11ts.get("Aermellaenge", "")
        if arm:
            result["Aermellaenge"] = norm_sleeve(arm)
        txt = attrs_11ts.get("Textillaenge", "")
        if txt:
            result["Textillaenge"] = norm_pants(txt)
        pf = attrs_11ts.get("Passform", "")
        if pf:
            result["Passform"] = norm_passform(pf)
        result["Serie"] = attrs_11ts.get("Serie", "")
        result["Produktart"] = attrs_11ts.get("Produktart", "")

        # Material composition
        mat1 = attrs_11ts.get("Material_1", "")
        pct1 = attrs_11ts.get("Material_1_pct", "")
        mat2 = attrs_11ts.get("Material_2", "")
        pct2 = attrs_11ts.get("Material_2_pct", "")
        parts = []
        if mat1:
            parts.append(f"{pct1}% {mat1}" if pct1 else mat1)
        if mat2:
            parts.append(f"{pct2}% {mat2}" if pct2 else mat2)
        result["Material"] = ", ".join(parts)

        # Supplement from MCP if 11ts is missing some apparel attrs
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            if not result["Aermellaenge"]:
                sl = attrs_mcp.get("MCP_Sleeve_length", "")
                if sl:
                    result["Aermellaenge"] = norm_sleeve(sl)
            if not result["Textillaenge"]:
                pc = attrs_mcp.get("MCP_Pants_cut", "")
                if pc:
                    result["Textillaenge"] = norm_pants(pc)
            if not result["Padding"]:
                result["Padding"] = attrs_mcp.get("MCP_Padding", "")

        return result

    # ── Priority 2: KS house brand ──
    if marke == "KEEPERsport" and "-" in sku:
        fc = sku.rsplit("-", 1)[1]
        mcp_colors = []
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            mcp_colors = [c.strip() for c in attrs_mcp.get("MCP_Color_keys", "").split(",") if c.strip()]

        # KS Farbcode mapping
        if fc in KS_FARBCODE_BASISFARBE:
            result["Basisfarbe"] = KS_FARBCODE_BASISFARBE[fc]
            if mcp_colors:
                for c in [norm(x) for x in mcp_colors]:
                    if c != result["Basisfarbe"] and not result["Highlight_1"]:
                        result["Highlight_1"] = c
                    elif c != result["Basisfarbe"] and c != result["Highlight_1"] and not result["Highlight_2"]:
                        result["Highlight_2"] = c
            result["Color_confidence"] = "high"
            result["Resolution_source"] = "ks_katalog"
        elif mcp_colors:
            result["Basisfarbe"] = norm(mcp_colors[0])
            result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
            result["Color_confidence"] = "medium"
            result["Resolution_source"] = "mcp"
        else:
            # Phase 1 Farbcode
            fc_data = fc_map.get(fc)
            if fc_data:
                api_primary = fc_data.get("Farben_API_primary", "").strip()
                if api_primary:
                    result["Basisfarbe"] = norm(api_primary)
                    result["Color_confidence"] = "medium"
                    result["Resolution_source"] = "ks_farbcode"

        # Apparel attrs from MCP
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            sl = attrs_mcp.get("MCP_Sleeve_length", "")
            if sl:
                result["Aermellaenge"] = norm_sleeve(sl)
            pc = attrs_mcp.get("MCP_Pants_cut", "")
            if pc:
                result["Textillaenge"] = norm_pants(pc)
            result["Padding"] = attrs_mcp.get("MCP_Padding", "")
            result["Serie"] = attrs_mcp.get("MCP_Collection", "")
            pf = attrs_mcp.get("MCP_Fit", "")
            if pf:
                result["Passform"] = norm_passform(pf)

        return result

    # ── Priority 3: rehab ──
    if marke == "rehab":
        mcp_colors = []
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            mcp_colors = [c.strip() for c in attrs_mcp.get("MCP_Color_keys", "").split(",") if c.strip()]
            if mcp_colors:
                result["Basisfarbe"] = norm(mcp_colors[0])
                result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
                result["Color_confidence"] = "medium"
                result["Resolution_source"] = "mcp"
            result["Padding"] = attrs_mcp.get("MCP_Padding", "")
            pc = attrs_mcp.get("MCP_Pants_cut", "")
            if pc:
                result["Textillaenge"] = norm_pants(pc)
            sl = attrs_mcp.get("MCP_Sleeve_length", "")
            if sl:
                result["Aermellaenge"] = norm_sleeve(sl)
            result["Serie"] = attrs_mcp.get("MCP_Collection", "")
        return result

    # ── Priority 4: Other brands, MCP only ──
    if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
        mcp_colors = [c.strip() for c in attrs_mcp.get("MCP_Color_keys", "").split(",") if c.strip()]
        if mcp_colors:
            result["Basisfarbe"] = norm(mcp_colors[0])
            result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
        result["Farbbezeichnung_Hersteller"] = attrs_mcp.get("MCP_Color_labels", "")
        result["Color_confidence"] = "medium"
        result["Resolution_source"] = "mcp"
        sl = attrs_mcp.get("MCP_Sleeve_length", "")
        if sl:
            result["Aermellaenge"] = norm_sleeve(sl)
        pc = attrs_mcp.get("MCP_Pants_cut", "")
        if pc:
            result["Textillaenge"] = norm_pants(pc)
        result["Padding"] = attrs_mcp.get("MCP_Padding", "")
        result["Serie"] = attrs_mcp.get("MCP_Collection", "")
        pf = attrs_mcp.get("MCP_Fit", "")
        if pf:
            result["Passform"] = norm_passform(pf)

    return result


# ── Output generation ─────────────────────────────────────────────────────

CATEGORY_FILES = {
    "shirts": ROOT / "gk_shirts_aggregated.csv",
    "pants": ROOT / "gk_pants_aggregated.csv",
    "baselayers": ROOT / "gk_baselayers_aggregated.csv",
    "sets": ROOT / "gk_sets_aggregated.csv",
}


def main() -> None:
    print("Loading data sources...")
    attrs_11ts = load_11ts_attrs()
    attrs_mcp = load_mcp_attrs()
    fc_map = load_ks_farbcode_map()

    print(f"  11ts attrs: {len(attrs_11ts)} SKUs")
    print(f"  MCP attrs:  {len(attrs_mcp)} SKUs")
    print(f"  KS Farbcodes: {len(fc_map)} codes")

    quarters = [f"{y}-Q{q}" for y in range(2022, 2027) for q in range(1, 5)]
    quarters = [q for q in quarters if q <= "2026-Q2"]
    years = [str(y) for y in range(2022, 2027)]

    header = [
        "Marke_Code", "Marke", "ParentSKU", "ProductName",
        "Kategorie_Code", "Kategorie",
        "Basisfarbe", "Highlight_1", "Highlight_2", "Farbbezeichnung_Hersteller",
        "Aermellaenge", "Textillaenge", "Passform", "Padding",
        "Material", "Serie", "Produktart",
        "Color_confidence", "Resolution_source",
        "Units_total", "Revenue_total",
    ]
    for y in years:
        header += [f"Units_{y}", f"Revenue_{y}"]
    for q in quarters:
        header += [f"Units_{q}", f"Revenue_{q}"]

    for cat_key, agg_path in CATEGORY_FILES.items():
        if not agg_path.exists():
            print(f"\nSKIP {cat_key} — {agg_path.name} not found")
            continue

        agg = load_agg(agg_path)
        print(f"\n{'='*70}")
        print(f"Processing {cat_key} ({len(agg)} SKUs)")

        conf_counts = Counter()
        src_counts = Counter()
        rows = []

        for sku, agg_row in agg.items():
            marke = agg_row["Marke"]
            colors = resolve_apparel(
                sku, marke,
                attrs_11ts.get(sku),
                attrs_mcp.get(sku),
                fc_map,
            )

            conf_counts[colors["Color_confidence"]] += 1
            src_counts[colors["Resolution_source"]] += 1

            row = {
                "Marke_Code": agg_row["Marke_Code"],
                "Marke": marke,
                "ParentSKU": sku,
                "ProductName": agg_row["ProductName"],
                "Kategorie_Code": agg_row["Kategorie_Code"],
                "Kategorie": agg_row["Kategorie"],
                **colors,
                "Units_total": agg_row["Units_total"],
                "Revenue_total": agg_row["Revenue_total"],
            }
            for y in years:
                row[f"Units_{y}"] = agg_row.get(f"Units_{y}", 0)
                row[f"Revenue_{y}"] = agg_row.get(f"Revenue_{y}", 0)
            for q in quarters:
                row[f"Units_{q}"] = agg_row.get(f"Units_{q}", 0)
                row[f"Revenue_{q}"] = agg_row.get(f"Revenue_{q}", 0)
            rows.append(row)

        rows.sort(key=lambda r: -float(r["Revenue_total"]))

        out = ROOT / f"gk_{cat_key}_enriched.csv"
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            w.writerows(rows)

        total_rev = sum(float(r["Revenue_total"]) for r in rows)
        print(f"Wrote {out.name} ({len(rows)} rows × {len(header)} cols)")

        print(f"\nColor_confidence:")
        for conf in ["high", "medium", "low"]:
            n = conf_counts[conf]
            rev = sum(float(r["Revenue_total"]) for r in rows if r["Color_confidence"] == conf)
            pct = rev / total_rev * 100 if total_rev else 0
            print(f"  {conf:<8}: {n:>5} SKUs  EUR {rev:>12,.0f}  ({pct:>5.1f}%)")

        print(f"\nResolution_source:")
        for src, n in src_counts.most_common():
            rev = sum(float(r["Revenue_total"]) for r in rows if r["Resolution_source"] == src)
            pct = rev / total_rev * 100 if total_rev else 0
            print(f"  {src:<20}: {n:>5} SKUs  EUR {rev:>12,.0f}  ({pct:>5.1f}%)")

        with_color = sum(1 for r in rows if r["Basisfarbe"])
        rev_with = sum(float(r["Revenue_total"]) for r in rows if r["Basisfarbe"])
        pct = rev_with / total_rev * 100 if total_rev else 0
        print(f"\nBasisfarbe coverage: {with_color}/{len(rows)} SKUs ({pct:.1f}% of revenue)")

        # Apparel-specific coverage
        for attr in ["Aermellaenge", "Textillaenge", "Passform", "Padding", "Material", "Serie"]:
            n = sum(1 for r in rows if r[attr])
            print(f"  {attr:<15}: {n:>4} / {len(rows)}")


if __name__ == "__main__":
    main()
