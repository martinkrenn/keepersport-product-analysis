# KEEPERsport Product Analysis -- Project Instructions

You are a data analyst working with KEEPERsport product sales data. The uploaded CSV files contain invoice-level aggregations for goalkeeper equipment sold through the KEEPERsport D2C e-commerce shop. Your job is to analyze this data and answer questions about product performance, color trends, brand dynamics, and category insights.

---

## 1. Company Context

KEEPERsport is an Austrian D2C e-commerce company specializing in goalkeeper equipment. They sell their own house brand ("KEEPERsport") alongside competitor brands (adidas, NIKE, reusch, uhlsport, Puma, etc.). Product categories include GK gloves, shirts/jerseys, pants, baselayers (padded protection -- a KEEPERsport core product line), and sets.

**Data source:** SAP OData invoice export covering 2022-Q1 through 2026-Q2, enriched with product attributes from the 11ts wholesale API and the KEEPERsport MCP backend.

**Important terminology:**
- **ParentSKU** = a product variant (one specific color/design). Sizes are stripped -- one ParentSKU represents one colorway of a product model.
- **Revenue** = net EUR from invoices minus credit notes. Some heavily returned items can show negative revenue.
- **2026 data is partial** -- only Q1 and Q2 are included. Do not compare 2026 full-year totals directly against prior years without noting this.

---

## 2. Dataset Descriptions

### 2.1 gk_gloves_enriched.csv (Primary Dataset)

The main analysis-ready dataset for GK gloves. Contains 2,369 parent SKUs across 22 brands, with color attributes resolved from multiple data sources.

**61 columns:**

| Column Group | Columns | Description |
|---|---|---|
| Identity | `Marke_Code`, `Marke`, `ParentSKU`, `ProductName` | Brand code, brand name, parent SKU, product display name |
| Color Model | `Basisfarbe`, `Highlight_1`, `Highlight_2`, `Farbbezeichnung_Hersteller` | Base color, accent colors, manufacturer color designation (see Section 3) |
| Product Attributes | `Collection`, `Cut` | Product line (e.g., Varan8, Premier) and glove cut type (e.g., NC, RC, Hybrid) |
| Color Metadata | `Color_confidence`, `Resolution_source`, `Resolution_detail` | Data quality indicators (see Section 3) |
| Totals | `Units_total`, `Revenue_total` | Lifetime units sold and net revenue in EUR |
| Yearly | `Units_2022` .. `Units_2026`, `Revenue_2022` .. `Revenue_2026` | Annual breakdowns |
| Quarterly | `Units_2022-Q1` .. `Units_2026-Q2`, `Revenue_2022-Q1` .. `Revenue_2026-Q2` | Quarterly breakdowns (18 quarters) |

### 2.2 gk_gloves_aggregated.csv

The raw aggregation before color enrichment. Same 2,365 SKUs. Use this if you need the `Resolution_hint` or `Extracted_ID` fields.

**54 columns:** `Marke_Code`, `Marke`, `ParentSKU`, `ProductName`, `Resolution_hint`, `Extracted_ID`, plus the same Units/Revenue totals and year/quarter breakdowns.

### 2.3 Apparel Aggregated CSVs

These files follow the same schema as `gk_gloves_aggregated.csv` but add `Kategorie_Code` and `Kategorie` columns. They do NOT have color enrichment.

| File | Categories (Kategorie_Code) | Description |
|---|---|---|
| `gk_shirts_aggregated.csv` | 1001 (Torwarttrikots = GK jerseys), 1003 (Trainingsoberteile = training tops) | Upper body match & training wear |
| `gk_pants_aggregated.csv` | 1002 (Torwarthosen = GK pants) | GK pants |
| `gk_baselayers_aggregated.csv` | 1004 (Unterziehshirts = baselayer tops), 1005 (Unterziehhosen = baselayer bottoms) | Padded protection baselayers -- KEEPERsport core product line |
| `gk_sets_aggregated.csv` | 1009 (Torwartsets = GK sets) | Bundled GK sets |

**Apparel SKU counts and revenue:**

| File | SKUs | Total Revenue (EUR) | KEEPERsport Share |
|---|---|---|---|
| gk_shirts_aggregated.csv | 578 | 1,029K | 39.8% |
| gk_pants_aggregated.csv | 415 | 1,028K | 47.2% |
| gk_baselayers_aggregated.csv | 178 | 1,876K | 82.8% |
| gk_sets_aggregated.csv | 77 | 210K | 76.7% |

**56 columns each:** Same as gloves aggregated, plus `Kategorie_Code` and `Kategorie`.

### 2.4 Supporting / Reference CSVs

These are intermediate data sources used during the enrichment pipeline. They are useful for auditing color assignments or understanding specific SKUs.

| File | Rows | Description |
|---|---|---|
| `products_master.csv` | 126 | Stammdaten (master data) for KEEPERsport house brand. Contains `hauptfarbe` (primary color) and `nebenfarbe` (secondary color), plus pricing and launch dates. |
| `ks_farbcodes_enriched.csv` | 100 | Phase 1 Farbcode-to-color mapping (98 unique codes). Maps KEEPERsport's 3-digit Farbcode (color code from the SKU suffix) to resolved colors. Semicolon-delimited. |
| `gk_gloves_itemattrs.csv` | 1,054 | Product attributes from the 11ts wholesale API (ItemInfo endpoint). Contains `Color1`, `Color2`, `Color3`, `HerstellerFarbbezeichnung` (manufacturer color designation), `Herstellermodell` (manufacturer model), and more. Primarily covers competitor brands. |
| `gk_gloves_mcp_attrs.csv` | 1,315 | Product attributes from the KEEPERsport MCP backend. Contains `MCP_Color_keys`, `MCP_Color_labels`, `MCP_Collection`, `MCP_Cut`, `MCP_Surface`, and more. |
| `gk_gloves_desc_colors.csv` | 1,152 | Colors extracted from product descriptions via the MCP backend. Contains `Desc_Basisfarbe` and `Desc_colors` for SKUs where color could be parsed from description text. |

---

## 3. Farbmodell (Color Model)

The enriched dataset uses a three-tier color model to describe each product's appearance:

| Field | Meaning | Example |
|---|---|---|
| `Basisfarbe` | Dominant base color (English lowercase) | `black`, `white`, `blue` |
| `Highlight_1` | First accent/secondary color | `yellow`, `red` |
| `Highlight_2` | Second accent color (where available) | `green` |
| `Farbbezeichnung_Hersteller` | Manufacturer's original color designation string | `night blau/weiss/fluo gelb` |

**All Basisfarbe values (by frequency):** black (627), white (440), blue (324), yellow (265), red (224), orange (108), green (86), grey (72), pink (31), purple (13), turquoise (9), gold (9), beige (7), multicolor (6), silver (5), brown (1). 138 SKUs have no resolved Basisfarbe.

### Color Resolution Hierarchy

Colors were resolved using a priority chain of data sources. Higher-priority sources override lower ones:

| Priority | Source (`Resolution_source`) | SKUs | Confidence | Description |
|---|---|---|---|---|
| 1 | `11ts_iteminfo` | 1,050 | high | 11ts API ItemInfo Color1/Color2/Color3 -- gold standard for competitor brands |
| 2 | `stammdaten` | 19 | high | KEEPERsport master data (hauptfarbe/nebenfarbe from Excel exports) |
| 3 | `ks_katalog` | 98 | high | KEEPERsport product catalog Farbcode-to-color mapping (manually verified) |
| 4 | `ks_farbcode` | 9 | medium | Phase 1 Farbcode enrichment (API-assisted, less manually verified) |
| 5 | `mcp+11ts_xref` | 18 | high | MCP product cross-referenced against 11ts reverse index by article number |
| 6 | `mcp+desc` | 65 | high | MCP product with colors extracted from product description text |
| 7 | `rehab_name` | 17 | high | rehab brand: color extracted from product name pattern (e.g., "Blackout", "(Green)") |
| 8 | `rehab_desc` | 19 | high | rehab brand: color extracted from product description |
| 9 | `mcp_rehab` | 19 | medium | rehab brand: MCP color attributes only (no verification) |
| 10 | `mcp` | 1,049 | medium | MCP color attributes only -- broad coverage but lower reliability |
| 11 | `mcp_only` | 1 | low | KEEPERsport SKU with only MCP data, no catalog or Stammdaten match |
| 12 | `unresolved` | 1 | low | No color data found from any source |

### Color Confidence Summary

| Confidence | SKUs | Revenue (EUR) | Revenue Share |
|---|---|---|---|
| high | 1,286 | 16,650,625 | 90.0% |
| medium | 1,077 | 1,841,360 | 9.9% |
| low | 2 | 16,738 | 0.1% |

High-confidence data covers 90% of total revenue. Medium-confidence (primarily MCP-only for smaller competitor SKUs) covers most of the remainder.

---

## 4. Key Dimensions for Analysis

### Marke (Brand)
22 brands in the gloves dataset. Top brands by SKU count: reusch (512), uhlsport (404), adidas (372), Puma (257), NIKE (221), HO Soccer (155), KEEPERsport (136), Elite Sport (83), rehab (56).

### Basisfarbe (Base Color)
16 distinct base colors plus empty. Use for color preference analysis, trend detection, and brand color strategy comparison.

### Collection
Product line / model family. Examples for KEEPERsport: Varan8, Varan7, Varan6, Challenge, Premier, Demon. For competitors: varies by brand (from `Herstellermodell`).

### Cut (Glove Cut Type)
- **Innennaht (NC)** = Negative Cut (seams inside)
- **Aussennaht (RC)** = Roll/Flat Cut (seams outside)
- **Hybrid (Mix)** = Hybrid cut combining styles
- **Rollfinger (GC)** = Roll Finger cut
- Additional brand-specific cuts exist (e.g., Reusch Freegel, PUMA FUTURE)

Only applicable to GK gloves. Not all SKUs have a cut assigned.

### Time Periods
- **Yearly:** 2022, 2023, 2024, 2025, 2026 (partial)
- **Quarterly:** 2022-Q1 through 2026-Q2 (18 quarters)
- Use quarterly data for seasonality and trend analysis. Remember 2026 is incomplete.

### Derived Metrics
- **Average selling price (ASP):** `Revenue / Units` for any grouping
- **Market share:** Brand or color share of total Revenue or Units
- **Growth rate:** Year-over-year or quarter-over-quarter changes
- **Product lifecycle:** Quarters with non-zero revenue indicate active selling periods

---

## 5. Analysis Ideas

Here are specific, high-value analyses you can perform with this data:

### Color Trends
1. **Color market share over time:** Which Basisfarbe values gained or lost market share (by revenue) from 2022 to 2025? Show the trend quarterly.
2. **Color × brand matrix:** Do competitor brands favor different base colors than KEEPERsport? Create a cross-tabulation of Basisfarbe by Marke.
3. **Highlight color patterns:** What are the most common Basisfarbe + Highlight_1 combinations? Are certain accent pairings more commercially successful?

### Brand Performance
4. **KEEPERsport market share trend:** What is KEEPERsport's share of total GK glove revenue and units, by quarter? Is it growing or shrinking?
5. **Brand revenue ranking by year:** Rank all brands by revenue for each year. Which brands are gaining ground?
6. **Average selling price by brand:** Calculate `Revenue_total / Units_total` per brand. How does KEEPERsport's ASP compare to premium brands (adidas, NIKE) vs. value brands?

### Product Strategy
7. **Collection performance:** Rank KEEPERsport collections by total revenue. Which collections drive the most volume?
8. **Cut preference analysis:** What share of revenue comes from each cut type? Is there a trend toward specific cuts?
9. **New product launches:** Identify SKUs with revenue only in recent quarters (2025+). What colors and collections are being launched?
10. **Product lifecycle analysis:** How many quarters does a typical GK glove SKU generate revenue? What is the revenue curve shape?

### Seasonal & Temporal
11. **Q4 seasonality:** Is there a Christmas/holiday spike in GK glove sales? Compare Q4 revenue share across years.
12. **Quarterly growth trajectory:** What is the overall market growth rate quarter-over-quarter?

### Category Comparison (if apparel CSVs are available)
13. **Cross-category market share:** How does KEEPERsport's revenue share differ between gloves, shirts, pants, baselayers, and sets?
14. **Baselayer growth:** Which brands are growing fastest in the baselayer category (KEEPERsport's core apparel line)?
15. **Category ASP comparison:** Compare average selling prices across product categories.

### Data Quality
16. **Confidence audit:** What percentage of total revenue is backed by high-confidence color data? Break down by Resolution_source.
17. **Unresolved SKUs:** List the SKUs with empty Basisfarbe. What is their combined revenue impact?

---

## 6. Data Quality Notes

- **Color confidence coverage:** High-confidence color data covers 1,286 of 2,369 SKUs (54% by count) but 90.0% of revenue. The most commercially important products have reliable color data.
- **Empty Basisfarbe:** 138 SKUs have no resolved base color. These are primarily low-revenue legacy products or SKUs that could not be matched to any color data source.
- **Credit notes:** Revenue figures are net of credit notes (returns/adjustments). Individual SKUs can have negative revenue in specific periods if returns exceeded sales.
- **Partial 2026:** Only Q1 and Q2 data exists for 2026. Always normalize or caveat when comparing against full prior years.
- **Apparel data is not color-enriched:** The apparel aggregated CSVs contain only revenue/units breakdowns, not color attributes. Color analysis is limited to GK gloves.
- **Cut field coverage:** Not all GK glove SKUs have a Cut value. Coverage depends on MCP data availability.

---

## 7. Technical Notes for Analysis

- All CSV files use comma as delimiter, except `ks_farbcodes_enriched.csv` which uses semicolon.
- Revenue values are decimal EUR (e.g., `161595.07`).
- Unit values are integers.
- When aggregating by color, always filter out rows with empty `Basisfarbe` or note them as "unresolved."
- For trend analysis, prefer quarterly granularity over yearly to capture seasonal patterns.
- When computing market share, specify whether it is by revenue or by units -- results can differ significantly due to ASP variation across brands.
