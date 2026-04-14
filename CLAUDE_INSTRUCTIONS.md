# KEEPERsport Product Analysis -- Projektanweisungen

## Unternehmen

KEEPERsport ist ein oesterreichischer D2C-E-Commerce-Spezialist fuer Torwartausruestung. Neben der Hausmarke "KEEPERsport" werden Konkurrenzmarken (adidas, NIKE, reusch, uhlsport, Puma u.a.) vertrieben. Kategorien: TW-Handschuhe, Trikots, Hosen, Baselayers (gepolsterte Schutzunterwaesche -- KEEPERsport-Kernprodukt, EUR 1.88M Umsatz, 82.8% KS-Anteil), Sets.

**Datenquelle:** SAP-OData-Rechnungsexport 2022-Q1 bis 2026-Q2, angereichert mit Produktattributen aus der 11ts-Wholesale-API und dem KEEPERsport-MCP-Backend. 2026-Daten sind partial (nur Q1+Q2) -- bei Jahresvergleichen immer Caveat nennen.

**Terminologie:**
- **ParentSKU** = eine Produktvariante (ein Farbweg/Design). Groessen sind aggregiert -- ein ParentSKU steht fuer eine Farbvariante eines Produktmodells.
- **Revenue** = Netto-EUR aus Rechnungen abzgl. Gutschriften. Einzelne SKUs koennen negative Umsaetze aufweisen.

---

## Arbeitsweise

Bei Analysefragen: bash_tool nutzen, Python/pandas auf den CSV-Dateien laufen lassen. Nicht konzeptuell antworten wenn direkte Datenabfrage moeglich ist. Sprache: Deutsch. Output: Tabellen wo sinnvoll, Zahlen mit einer Dezimalstelle, EUR-Betraege in Tausend (K) oder Millionen (M).

---

## Datasets

Alle Dateien liegen unter `data/`. Komma-getrennt, Revenue als Dezimal-EUR, Units als Integer.

| Datei | Kategorie | SKUs | Marken | Spalten | Hinweise |
|---|---|---|---|---|---|
| `gk_gloves_enriched.csv` | TW-Handschuhe | 2,365 | 22 | 61 | Hauptdatensatz. Inkl. Collection, Cut |
| `gk_shirts_enriched.csv` | Trikots + Trainingsoberteile | 578 | -- | 67 | Kategorie 1001+1003 |
| `gk_pants_enriched.csv` | TW-Hosen | 415 | -- | 67 | Kategorie 1002 |
| `gk_baselayers_enriched.csv` | Unterzieh-Shirts + -Hosen | 178 | -- | 67 | Kategorie 1004+1005 |
| `gk_sets_enriched.csv` | TW-Sets | 77 | -- | 67 | Kategorie 1009 |

**Gemeinsame Spaltenstruktur:**
Identity (`Marke`, `ParentSKU`, `ProductName`), Farbmodell (`Basisfarbe`, `Highlight_1`, `Highlight_2`, `Farbbezeichnung_Hersteller`), Qualitaet (`Color_confidence`, `Resolution_source`), Aggregate (`Units_total`, `Revenue_total`), Jahres- und Quartalswerte (2022--2026, 18 Quartale). Apparel-Dateien zusaetzlich: `Kategorie_Code`, `Kategorie`, `Aermellaenge`, `Textillaenge`, `Passform`, `Padding`, `Material`, `Serie`, `Produktart`. Gloves zusaetzlich: `Collection`, `Cut`.

---

## Farbmodell (Color Model)

Dreistufiges Modell pro Produkt:

| Feld | Bedeutung | Beispiel |
|---|---|---|
| `Basisfarbe` | Dominante Grundfarbe (englisch, lowercase) | `black`, `blue` |
| `Highlight_1` | Erste Akzentfarbe | `yellow` |
| `Highlight_2` | Zweite Akzentfarbe (falls vorhanden) | `green` |

**Farbzuverlaessigkeit (Color_confidence):**

| Confidence | Revenue-Anteil | Bedeutung |
|---|---|---|
| high | 90.0% | Verifizierte Quellen (11ts API, Stammdaten, Katalog) |
| medium | 9.9% | MCP-only oder weniger verifiziert |
| low | 0.1% | Keine belastbare Quelle |

---

## Analysedimensionen

- **Marke**: 22 Marken bei Handschuhen. Top: reusch, uhlsport, adidas, Puma, NIKE, HO Soccer, KEEPERsport
- **Basisfarbe**: 16 Farben + leer. Leere Basisfarbe bei Aggregationen ausfiltern oder als "unresolved" kennzeichnen
- **Collection**: Produktlinie/Modellfamilie (z.B. Varan8, Challenge, Premier)
- **Cut** (nur Handschuhe): 4 Haupttypen -- Negative Cut (Innennaht), Regular Cut (Aussennaht), Hybrid (Mix), Rollfinger
- **Apparel-Schnitt**: Aermellaenge (Langarm/Kurzarm), Textillaenge (Lang/Kurz/3_4/Normal), Passform (eng/schmal/normal/weit), Padding (elbow/hip/knee)
- **Zeit**: Jahres- und Quartalsgranularitaet. Quartalsdaten fuer Saisonalitaet bevorzugen

---

## Datenqualitaet

- 138 Handschuh-SKUs ohne Basisfarbe (primaer umsatzschwache Altprodukte)
- Revenue ist netto nach Gutschriften; einzelne SKUs koennen negative Werte haben
- Apparel Basisfarbe-Abdeckung > 99% (Revenue). Apparel-Schnittattribute lueckenhaft: Aermellaenge ~79% (Shirts), Textillaenge ~69% (Hosen), Padding ~46% (Baselayers)
- Nicht alle Handschuh-SKUs haben einen Cut-Wert
- Marktanteilsberechnungen immer nach Revenue ODER Units spezifizieren (ASP-Unterschiede zwischen Marken)
