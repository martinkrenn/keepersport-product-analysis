#!/usr/bin/env python3
"""
Phase 2 Step 5 (v2): Merge all data sources into gk_gloves_enriched.csv.

Sources:
1. gk_gloves_aggregated.csv     — revenue × period per parent SKU (2365 rows)
2. gk_gloves_itemattrs.csv      — 11ts ItemInfo attrs for competitor SKUs (1050 rows)
3. gk_gloves_mcp_attrs.csv      — MCP getProduct attrs for everything else (1315 rows)
4. ks_farbcodes_enriched.csv    — Phase 1 KS Farbcode → color mapping (98 Farbcodes)
5. products_master.csv          — Stammdaten Excels (hauptfarbe/nebenfarbe, 126 rows)
6. .11ts_cache/itemid/*.xml     — 11ts reverse index for cross-referencing MCP SKUs

Color model:
  Basisfarbe   = dominant base color (the "main" color of the product)
  Highlight_1  = first accent color
  Highlight_2  = second accent color (from 11ts Color3 where available)
  Farbbezeichnung_Hersteller = manufacturer color designation string

Color_confidence:
  high   = 11ts Color1 | Stammdaten hauptfarbe | KS Katalog mapping | rehab name
  medium = MCP cross-referenced via 11ts reverse index | KS Phase 1 Farbcode enrichment
  low    = MCP-only, no verification
"""
from __future__ import annotations

import csv
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path("/Users/Martin/sap-cohort-analysis/products")

# ── Color normalization (all → English lowercase) ──────────────────────────

COLOR_NORMALIZE = {
    "schwarz": "black", "weiss": "white", "weiß": "white",
    "blau": "blue", "rot": "red", "gelb": "yellow",
    "gruen": "green", "grün": "green", "grau": "grey",
    "orange": "orange", "lila": "purple", "rosa": "pink",
    "tuerkis": "turquoise", "türkis": "turquoise",
    "silber": "silver", "gold": "gold", "beige": "beige",
    "braun": "brown", "mehrfarbig": "multicolor",
    "lightpink": "pink",
}


def norm(c: str) -> str:
    c = c.strip().lower()
    return COLOR_NORMALIZE.get(c, c)


# ── KS Kampagne → Farbcode Basisfarbe mapping ──────────────────────────────
# From product catalog 1H26 + user corrections.
# Format: farbcode (3-digit zero-padded) → basisfarbe
# NOTE: some farbcodes are silo-specific (108 = Dominance varies by Hero vs Champ).
# For those we use the most common interpretation in GK Gloves category.

KS_FARBCODE_BASISFARBE = {
    # Current collections (from Katalog 1H26)
    "000": "white",     # Whiteout
    "001": "white",     # Whiteout / Eagle
    "011": "black",     # Standard schwarz
    "108": "black",     # Dominance (Champ/Pro=schwarz/rot; Hero=rot but minority)
    "210": "black",     # Resist (schwarz + yellow/white inserts)
    "401": "blue",      # Aqua neu (white/blue)
    "452": "blue",      # Aqua alt (cyan blue)
    "630": "white",     # 25years (white + gold/black inserts)
    "804": "white",     # Demon Varan8 (weiß/blau)
    "903": "black",     # Demon (black + fluogreen inserts) / Thermo (black/red)
    "906": "black",     # Standard apparel schwarz (not used in GK Gloves)
    "907": "white",     # Varan7 Premier (white/green per MCP)
    "909": "black",     # Blackout variant
    "991": "black",     # Blackout / Standard schwarz
    # Varan7 era (from user input + MCP data)
    "010": "black",     # Schwarz (Handschuhe) — gelb is highlight
    "066": "white",     # Gamebreaker (weiß mit rot inserts per MCP + 11ts)
    "091": "white",     # Varan6 Challenge = weiß Oberhand + schwarz Inserts; Varan7 Whiteout = weiß
    "471": "black",     # Game of Power (schwarz + türkis accent)
    "407": "blue",      # (blue per MCP)
    "416": "blue",      # Aqua (white/blue, basis=blue for Aqua line)
    "901": "black",     # Blackout (Schwarz)
    "905": "black",     # Blackout variant
    "908": "black",     # Resist / GKSix (schwarz + gelb accents)
    "701": "white",     # (white/blue or red/white — white is basis per majority of MCP hits)
    "454": "blue",      # (blue variant)
    "563": "black",     # Resist (schwarz)
    "633": "black",     # Resist (schwarz)
    "406": "blue",      # Aqua variant
    "423": "blue",      # (blue per MCP)
    "425": "blue",      # (blue per MCP, Varan6)
    "555": "black",     # (schwarz)
    "557": "black",     # (schwarz)
    "413": "blue",      # (blue per MCP)
    "110": "black",     # (schwarz)
    "405": "blue",      # (blue)
    "166": "white",     # (white variant)
    "755": "red",       # Zone RC (rot/blau per Stammdaten)
    "111": "black",     # (Varan6 schwarz)
    "116": "white",     # Varan6 Premier (weiß)
    "700": "red",       # (rot)
    "760": "red",       # Coach Zone (rot)
}

# ── rehab name → color extraction ──────────────────────────────────────────

REHAB_NAME_COLORS = {
    "blackout": "black", "whiteout": "white",
    "(green)": "green", "(blue)": "blue", "(orange)": "orange",
    "(red)": "red", "(yellow)": "yellow", "(pink)": "pink",
    "(austria)": "red", "(germany)": "black",
    "black/fluoyellow": "black", "blue/black": "blue",
    "black/white": "black", "white/blue": "white",
}


def rehab_color_from_name(name: str) -> str | None:
    name_lower = name.lower()
    for pattern, color in REHAB_NAME_COLORS.items():
        if pattern in name_lower:
            return color
    return None


# ── Data loaders ───────────────────────────────────────────────────────────

def load_agg() -> dict[str, dict]:
    rows = {}
    with (ROOT / "gk_gloves_aggregated.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ParentSKU"]] = row
    return rows


def load_11ts_attrs() -> dict[str, dict]:
    rows = {}
    with (ROOT / "gk_gloves_itemattrs.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ParentSKU"]] = row
    return rows


def load_mcp_attrs() -> dict[str, dict]:
    rows = {}
    with (ROOT / "gk_gloves_mcp_attrs.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows[row["ParentSKU"]] = row
    return rows


def load_ks_farbcode_map() -> dict[str, dict]:
    fc_map = {}
    with (ROOT / "ks_farbcodes_enriched.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            fc = row.get("Farbcode", "").strip()
            if fc:
                fc_map[fc.zfill(3)] = row
    return fc_map


def load_desc_colors() -> dict[str, dict]:
    """Load description-extracted colors keyed by ParentSKU."""
    rows = {}
    path = ROOT / "gk_gloves_desc_colors.csv"
    if path.exists():
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["Status"] == "found" and row["Desc_Basisfarbe"]:
                    rows[row["ParentSKU"]] = row
    return rows


def load_11ts_extra_attrs() -> dict[str, dict]:
    """Load 11ts /attributes data (Schnitt, Material, Technologien) keyed by ParentSKU."""
    rows = {}
    path = ROOT / "gk_gloves_11ts_extra_attrs.csv"
    if path.exists():
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows[row["ParentSKU"]] = row
    return rows


def load_stammdaten() -> dict[str, dict]:
    """Load products_master.csv keyed by sku_parent."""
    rows = {}
    path = ROOT / "products_master.csv"
    if path.exists():
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows[row["sku_parent"]] = row
    return rows


def build_11ts_reverse_index() -> dict[str, dict]:
    """Scan .11ts_cache/itemid/*.xml → reverse index {ProducerArticleNumber → {Color1, Color2, Color3}}."""
    idx = {}
    cache_dir = ROOT / ".11ts_cache" / "itemid"
    if not cache_dir.exists():
        return idx
    for xml_file in cache_dir.glob("*.xml"):
        try:
            root = ET.parse(str(xml_file)).getroot()
        except ET.ParseError:
            continue
        for item in root.iter("Item"):
            par_el = item.find("ProducerArticleNumber")
            if par_el is None or not par_el.text:
                continue
            par = par_el.text.strip()
            c1 = item.find("Colors/Color1")
            c2 = item.find("Colors/Color2")
            c3 = item.find("Colors/Color3")
            idx[par] = {
                "Color1": (c1.get("Value", "") if c1 is not None else "").strip(),
                "Color2": (c2.get("Value", "") if c2 is not None else "").strip(),
                "Color3": (c3.get("Value", "") if c3 is not None else "").strip(),
            }
    return idx


def extract_article_number(sku: str) -> str | None:
    """Try to extract a ProducerArticleNumber from a legacy SKU.

    Patterns:
      E7221806       → 7221806
      ETS7221904_... → 7221904
      NGS3381-100    → NGS3381 (Nike)
      SGP151610      → SGP151610 (Sells)
      HO511281IT1    → HO511281IT1
      RPB70102103    → RPB70102103
      U1054          → 1054
      ADN8566        → ADN8566
    """
    # Known prefixes that are NOT part of the article number
    sku_clean = sku.split("_")[0] if "_" in sku else sku  # strip trailing _suffix
    sku_clean = sku_clean.split("-")[0] if "-" in sku_clean else sku_clean  # strip -color suffix

    # Strip ETS/E prefix for erima
    if sku_clean.startswith("ETS"):
        sku_clean = sku_clean[3:]
    elif sku_clean.startswith("E") and sku_clean[1:].isdigit():
        sku_clean = sku_clean[1:]

    return sku_clean if sku_clean else None


# ── Cut normalization ─────────────────────────────────────────────────────
# Canonical glove cuts: Negative Cut, Regular Cut, Hybrid, Rollfinger
# Sources use varying names; normalize all to canonical.

CUT_NORMALIZE = {
    "innennaht": "Negative Cut",
    "innennaht (nc)": "Negative Cut",
    "negative cut": "Negative Cut",
    "nc": "Negative Cut",
    "außennaht": "Regular Cut",
    "außennaht (rc)": "Regular Cut",
    "aussennaht": "Regular Cut",
    "regular cut": "Regular Cut",
    "rc": "Regular Cut",
    "hybrid": "Hybrid",
    "hybrid (mix)": "Hybrid",
    "mix": "Hybrid",
    "rollfinger": "Rollfinger",
    "rollfinger (gc)": "Rollfinger",
    "gunncut": "Rollfinger",
    "gc": "Rollfinger",
}

# Values that are NOT cuts (Passform / feature descriptors)
CUT_BLACKLIST = {"schmal", "breit", "ohne knöchelschutz", "normal"}


def norm_cut(raw: str) -> str:
    """Normalize a potentially comma-separated cut string to canonical names.
    Multi-cut combinations (2+ distinct cuts) = Hybrid."""
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split(",")]
    normalized = []
    for p in parts:
        key = p.lower()
        if key in CUT_BLACKLIST:
            continue
        mapped = CUT_NORMALIZE.get(key, "")
        if mapped and mapped not in normalized:
            normalized.append(mapped)
    if len(normalized) > 1:
        return "Hybrid"
    return normalized[0] if normalized else ""


# ── Main resolution logic ─────────────────────────────────────────────────

def resolve_colors(
    sku: str,
    marke: str,
    product_name: str,
    attrs_11ts: dict | None,
    attrs_mcp: dict | None,
    fc_map: dict,
    stammdaten: dict,
    reverse_idx: dict,
    desc_colors: dict,
) -> dict:
    """Resolve Basisfarbe + Highlights + confidence from all sources."""
    result = {
        "Basisfarbe": "",
        "Highlight_1": "",
        "Highlight_2": "",
        "Farbbezeichnung_Hersteller": "",
        "Collection": "",
        "Cut": "",
        "Color_confidence": "low",
        "Resolution_source": "unresolved",
        "Resolution_detail": "",
    }

    # ── Priority 1: 11ts ItemInfo (highest confidence for competitors) ──
    if attrs_11ts:
        result["Basisfarbe"] = norm(attrs_11ts.get("Color1", ""))
        result["Highlight_1"] = norm(attrs_11ts.get("Color2", ""))
        result["Highlight_2"] = norm(attrs_11ts.get("Color3", ""))
        result["Farbbezeichnung_Hersteller"] = attrs_11ts.get("HerstellerFarbbezeichnung", "")
        result["Collection"] = attrs_11ts.get("Herstellermodell", "")
        result["Color_confidence"] = "high"
        result["Resolution_source"] = "11ts_iteminfo"
        result["Resolution_detail"] = attrs_11ts.get("ItemID", "")
        # Supplement Collection/Cut from MCP
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            if not result["Collection"]:
                result["Collection"] = attrs_mcp.get("MCP_Collection", "")
            result["Cut"] = norm_cut(attrs_mcp.get("MCP_Cut", ""))
        return result

    # ── Priority 2: KS house brand (multi-layer) ──
    if marke == "KEEPERsport" and "-" in sku:
        fc = sku.rsplit("-", 1)[1]
        mcp_cut = ""
        mcp_collection = ""
        mcp_colors = []
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            mcp_cut = attrs_mcp.get("MCP_Cut", "")
            mcp_collection = attrs_mcp.get("MCP_Collection", "")
            mcp_colors = [c.strip() for c in attrs_mcp.get("MCP_Color_keys", "").split(",") if c.strip()]

        # Layer 1: Stammdaten hauptfarbe
        stamm = stammdaten.get(sku)
        if stamm and stamm.get("hauptfarbe", "").strip():
            result["Basisfarbe"] = norm(stamm["hauptfarbe"])
            result["Highlight_1"] = norm(stamm.get("nebenfarbe", ""))
            result["Farbbezeichnung_Hersteller"] = f"{stamm.get('hauptfarbe','')}/{stamm.get('nebenfarbe','')}"
            result["Color_confidence"] = "high"
            result["Resolution_source"] = "stammdaten"
            result["Resolution_detail"] = f"FC={fc}"
            result["Cut"] = norm_cut(mcp_cut)
            result["Collection"] = mcp_collection
            return result

        # Layer 2: MCP color (live shop data — first key = Basisfarbe)
        if mcp_colors:
            result["Basisfarbe"] = norm(mcp_colors[0])
            result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
            result["Highlight_2"] = norm(mcp_colors[2]) if len(mcp_colors) > 2 else ""
            result["Color_confidence"] = "high"
            result["Resolution_source"] = "mcp_ks"
            result["Resolution_detail"] = f"FC={fc}"
            result["Cut"] = norm_cut(mcp_cut)
            result["Collection"] = mcp_collection
            result["Farbbezeichnung_Hersteller"] = attrs_mcp.get("MCP_Color_labels", "") if attrs_mcp else ""
            return result

        # Layer 3: KS Kampagne→Farbcode mapping (fallback for delisted products)
        if fc in KS_FARBCODE_BASISFARBE:
            result["Basisfarbe"] = KS_FARBCODE_BASISFARBE[fc]
            result["Color_confidence"] = "high"
            result["Resolution_source"] = "ks_katalog"
            result["Resolution_detail"] = f"FC={fc}"
            result["Cut"] = norm_cut(mcp_cut)
            result["Collection"] = mcp_collection
            fc_data = fc_map.get(fc)
            if fc_data:
                vorschlag = fc_data.get("Vorschlag_Farbe_Kampagne", "").strip()
                kampagne = fc_data.get("Kampagne_Farbe_FINAL", "").strip()
                result["Farbbezeichnung_Hersteller"] = kampagne or vorschlag
            return result

        # Layer 4: Phase 1 Farbcode enrichment
        fc_data = fc_map.get(fc)
        if fc_data:
            vorschlag = fc_data.get("Vorschlag_Farbe_Kampagne", "").strip()
            kampagne = fc_data.get("Kampagne_Farbe_FINAL", "").strip()
            api_primary = fc_data.get("Farben_API_primary", "").strip()
            api_secondary = fc_data.get("Farben_API_secondary", "").strip()
            if api_primary:
                result["Basisfarbe"] = norm(api_primary)
                result["Highlight_1"] = norm(api_secondary)
            elif mcp_colors:
                result["Basisfarbe"] = norm(mcp_colors[0])
                result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
            result["Farbbezeichnung_Hersteller"] = kampagne or vorschlag
            result["Color_confidence"] = "medium"
            result["Resolution_source"] = "ks_farbcode"
            result["Resolution_detail"] = f"FC={fc}"
            result["Cut"] = norm_cut(mcp_cut)
            result["Collection"] = mcp_collection
            return result

        # Layer 4: MCP only (Varan6/7 legacy, no other source)
        if mcp_colors:
            result["Basisfarbe"] = norm(mcp_colors[0])
            result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
            result["Color_confidence"] = "low"
            result["Resolution_source"] = "mcp_only"
            result["Resolution_detail"] = f"FC={fc}"
            result["Cut"] = norm_cut(mcp_cut)
            result["Collection"] = mcp_collection
            return result

    # ── Priority 3: rehab name-based extraction ──
    if marke == "rehab":
        rehab_color = rehab_color_from_name(product_name)
        mcp_cut = ""
        mcp_collection = ""
        mcp_colors = []
        if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
            mcp_cut = attrs_mcp.get("MCP_Cut", "")
            mcp_collection = attrs_mcp.get("MCP_Collection", "")
            mcp_colors = [c.strip() for c in attrs_mcp.get("MCP_Color_keys", "").split(",") if c.strip()]

        # Try description first (most reliable for rehab)
        desc = desc_colors.get(sku)
        if desc:
            desc_cols = [c.strip() for c in desc.get("Desc_colors", "").split("/") if c.strip()]
            if desc_cols:
                result["Basisfarbe"] = desc_cols[0]
                result["Highlight_1"] = desc_cols[1] if len(desc_cols) > 1 else ""
                result["Color_confidence"] = "high"
                result["Resolution_source"] = "rehab_desc"
                result["Resolution_detail"] = desc.get("Desc_colors", "")
                result["Cut"] = norm_cut(mcp_cut)
                result["Collection"] = mcp_collection
                result["Farbbezeichnung_Hersteller"] = attrs_mcp.get("MCP_Color_labels", "") if attrs_mcp else ""
                return result

        if rehab_color:
            result["Basisfarbe"] = rehab_color
            # Highlights from MCP
            for c in [norm(x) for x in mcp_colors]:
                if c != rehab_color and not result["Highlight_1"]:
                    result["Highlight_1"] = c
            result["Color_confidence"] = "high"
            result["Resolution_source"] = "rehab_name"
            result["Resolution_detail"] = product_name[:40]
        elif mcp_colors:
            result["Basisfarbe"] = norm(mcp_colors[0])
            result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
            result["Color_confidence"] = "medium"
            result["Resolution_source"] = "mcp_rehab"
            result["Resolution_detail"] = product_name[:40]
        result["Cut"] = norm_cut(mcp_cut)
        result["Collection"] = mcp_collection
        result["Farbbezeichnung_Hersteller"] = attrs_mcp.get("MCP_Color_labels", "") if attrs_mcp else ""
        return result

    # ── Priority 4: MCP with 11ts reverse-index cross-reference + description ──
    if attrs_mcp and attrs_mcp.get("MCP_resolved") == "true":
        mcp_colors = [c.strip() for c in attrs_mcp.get("MCP_Color_keys", "").split(",") if c.strip()]
        result["Collection"] = attrs_mcp.get("MCP_Collection", "")
        result["Cut"] = norm_cut(attrs_mcp.get("MCP_Cut", ""))
        result["Farbbezeichnung_Hersteller"] = attrs_mcp.get("MCP_Color_labels", "")

        # Try cross-reference via ProducerArticleNumber in 11ts reverse index
        art_nr = extract_article_number(sku)
        if art_nr and art_nr in reverse_idx:
            ri = reverse_idx[art_nr]
            result["Basisfarbe"] = norm(ri["Color1"])
            result["Highlight_1"] = norm(ri["Color2"])
            result["Highlight_2"] = norm(ri["Color3"])
            result["Color_confidence"] = "high"
            result["Resolution_source"] = "mcp+11ts_xref"
            result["Resolution_detail"] = f"ArtNr={art_nr}"
            return result

        # Try description-extracted colors
        desc = desc_colors.get(sku)
        if desc:
            desc_cols = [c.strip() for c in desc.get("Desc_colors", "").split("/") if c.strip()]
            if desc_cols:
                result["Basisfarbe"] = desc_cols[0]
                result["Highlight_1"] = desc_cols[1] if len(desc_cols) > 1 else (norm(mcp_colors[1]) if len(mcp_colors) > 1 else "")
                result["Highlight_2"] = desc_cols[2] if len(desc_cols) > 2 else ""
                result["Color_confidence"] = "high"
                result["Resolution_source"] = "mcp+desc"
                result["Resolution_detail"] = desc.get("Desc_colors", "")
                return result

        # No cross-ref → MCP only
        if mcp_colors:
            result["Basisfarbe"] = norm(mcp_colors[0])
            result["Highlight_1"] = norm(mcp_colors[1]) if len(mcp_colors) > 1 else ""
        result["Color_confidence"] = "medium"
        result["Resolution_source"] = "mcp"
        result["Resolution_detail"] = attrs_mcp.get("MCP_Name", "")
        return result

    result["Resolution_source"] = "unresolved"
    return result


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    print("Loading data sources...")
    agg = load_agg()
    attrs_11ts = load_11ts_attrs()
    attrs_mcp = load_mcp_attrs()
    fc_map = load_ks_farbcode_map()
    stammdaten = load_stammdaten()
    reverse_idx = build_11ts_reverse_index()
    desc_colors_map = load_desc_colors()
    extra_11ts = load_11ts_extra_attrs()

    print(f"  Aggregated:       {len(agg)} SKUs")
    print(f"  11ts attrs:       {len(attrs_11ts)} SKUs")
    print(f"  11ts extra attrs: {len(extra_11ts)} SKUs")
    print(f"  MCP attrs:        {len(attrs_mcp)} SKUs")
    print(f"  KS Farbcodes:     {len(fc_map)} codes")
    print(f"  Stammdaten:       {len(stammdaten)} SKUs")
    print(f"  11ts reverse idx: {len(reverse_idx)} article numbers")
    print(f"  Desc colors:      {len(desc_colors_map)} SKUs")

    quarters = [f"{y}-Q{q}" for y in range(2022, 2027) for q in range(1, 5)]
    quarters = [q for q in quarters if q <= "2026-Q2"]
    years = [str(y) for y in range(2022, 2027)]

    header = [
        "Marke_Code", "Marke", "ParentSKU", "ProductName",
        "Basisfarbe", "Highlight_1", "Highlight_2", "Farbbezeichnung_Hersteller",
        "Collection", "Cut",
        "Color_confidence", "Resolution_source", "Resolution_detail",
        "Units_total", "Revenue_total",
    ]
    for y in years:
        header += [f"Units_{y}", f"Revenue_{y}"]
    for q in quarters:
        header += [f"Units_{q}", f"Revenue_{q}"]

    rows = []
    from collections import Counter
    conf_counts = Counter()
    src_counts = Counter()

    for sku, agg_row in agg.items():
        marke = agg_row["Marke"]

        colors = resolve_colors(
            sku, marke, agg_row["ProductName"],
            attrs_11ts.get(sku),
            attrs_mcp.get(sku),
            fc_map, stammdaten, reverse_idx, desc_colors_map,
        )

        conf_counts[colors["Color_confidence"]] += 1
        src_counts[colors["Resolution_source"]] += 1

        # Merge 11ts /attributes: Cut fallback, Material, Technologien
        extra = extra_11ts.get(sku, {})
        schnitt_11ts = extra.get("Schnitt", "")
        schnitt_11ts = norm_cut(schnitt_11ts)
        # Cut priority: MCP (KS expert curation) > 11ts Schnitt (fallback)
        if not colors["Cut"] and schnitt_11ts:
            colors["Cut"] = schnitt_11ts

        # Material composition from 11ts /attributes
        mat_parts = []
        for n in ("1", "2", "3"):
            mat = extra.get(f"Material_{n}", "")
            pct = extra.get(f"Material_{n}_pct", "")
            if mat:
                mat_parts.append(f"{pct}% {mat}" if pct else mat)
        material = ", ".join(mat_parts)
        technologien = extra.get("Technologien", "")

        row = {
            "Marke_Code": agg_row["Marke_Code"],
            "Marke": marke,
            "ParentSKU": sku,
            "ProductName": agg_row["ProductName"],
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

    out = ROOT / "gk_gloves_enriched.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)

    total_rev = sum(float(r["Revenue_total"]) for r in rows)
    print(f"\nWrote {out}")
    print(f"  {len(rows)} rows × {len(header)} columns")

    print(f"\nColor_confidence breakdown:")
    for conf in ["high", "medium", "low"]:
        n = conf_counts[conf]
        rev = sum(float(r["Revenue_total"]) for r in rows if r["Color_confidence"] == conf)
        print(f"  {conf:<8}: {n:>5} SKUs  EUR {rev:>12,.0f}  ({rev/total_rev*100:>5.1f}%)")

    print(f"\nResolution_source breakdown:")
    for src, n in src_counts.most_common():
        rev = sum(float(r["Revenue_total"]) for r in rows if r["Resolution_source"] == src)
        print(f"  {src:<20}: {n:>5} SKUs  EUR {rev:>12,.0f}  ({rev/total_rev*100:>5.1f}%)")

    with_color = sum(1 for r in rows if r["Basisfarbe"])
    rev_with = sum(float(r["Revenue_total"]) for r in rows if r["Basisfarbe"])
    print(f"\nBasisfarbe coverage: {with_color}/{len(rows)} SKUs ({rev_with/total_rev*100:.1f}% of revenue)")

    with_cut = sum(1 for r in rows if r["Cut"])
    rev_cut = sum(float(r["Revenue_total"]) for r in rows if r["Cut"])
    print(f"Cut coverage: {with_cut}/{len(rows)} SKUs ({rev_cut/total_rev*100:.1f}% of revenue)")


if __name__ == "__main__":
    main()
