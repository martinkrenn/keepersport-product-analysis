#!/usr/bin/env python3
"""
Fetch MCP product descriptions for medium-confidence SKUs and extract color info.

Pattern: "in schwarz-blau", "in Schwarz/Gelb", "Farbe: rot" etc.
Output: gk_gloves_desc_colors.csv with extracted Basisfarbe from description text.
"""
from __future__ import annotations

import csv
import json
import re
import subprocess
import time
from html import unescape
from pathlib import Path

ROOT = Path("/Users/Martin/sap-cohort-analysis/products")
OUT = ROOT / "gk_gloves_desc_colors.csv"
CACHE = ROOT / ".11ts_cache" / "mcp_desc"

MCP_URL = "https://backend.keepersport.at/backend/mcp?token=07Zjh9C7lTwX5WfC0Kg4PwD1dZ5UHx6Y8eliWZuXxIIPXMkFOxtDF738dX6H1cEg"
MCP_AUTH = "Basic bWtyZW5uOnplcnNkeWtu"

COLOR_DE = {
    "schwarz": "black", "weiss": "white", "weiß": "white",
    "blau": "blue", "rot": "red", "gelb": "yellow",
    "grün": "green", "gruen": "green", "grau": "grey",
    "orange": "orange", "lila": "purple", "rosa": "pink",
    "pink": "pink", "türkis": "turquoise", "tuerkis": "turquoise",
    "silber": "silver", "gold": "gold", "beige": "beige",
    "braun": "brown",
    # English
    "black": "black", "white": "white", "blue": "blue",
    "red": "red", "yellow": "yellow", "green": "green",
    "grey": "grey", "gray": "grey", "volt": "yellow",
    "cyan": "blue", "navy": "blue", "neon": "yellow",
}

# Patterns that indicate actual colors in text (not model names like "Gold X")
# Look for: "in schwarz-blau", "in Schwarz/Gelb", "Farbe: rot", "farblich in schwarz"
COLOR_CONTEXT_PATTERNS = [
    # "in schwarz-blau" / "in Schwarz und Gelb" / "in der Farbe schwarz"
    r'\bin\s+(?:der\s+Farbe\s+)?(\w+)(?:[\s/\-]+(\w+))?(?:[\s/\-]+(\w+))?',
    # "Farbe: schwarz" / "Farbkombination schwarz-gelb"
    r'[Ff]arb\w*[\s:]+(\w+)(?:[\s/\-]+(\w+))?(?:[\s/\-]+(\w+))?',
    # "schwarz-gelb" at word boundary (hyphenated color pair)
    r'\b(\w+)-(\w+)\b',
]


def extract_colors_from_text(html: str) -> list[str]:
    """Extract normalized color words from description HTML, respecting context."""
    # Strip HTML tags
    text = re.sub(r'<[^>]+>', ' ', unescape(html))
    text = re.sub(r'\s+', ' ', text).strip()

    found_colors = []

    # Strategy 1: Look for "in COLOR" or "Farbe COLOR" patterns
    for pat in COLOR_CONTEXT_PATTERNS[:2]:
        for m in re.finditer(pat, text, re.IGNORECASE):
            for g in m.groups():
                if g and g.lower() in COLOR_DE:
                    c = COLOR_DE[g.lower()]
                    if c not in found_colors:
                        found_colors.append(c)

    if found_colors:
        return found_colors

    # Strategy 2: Look for hyphenated color pairs like "schwarz-blau"
    for m in re.finditer(r'\b(\w+)-(\w+)\b', text):
        w1, w2 = m.group(1).lower(), m.group(2).lower()
        if w1 in COLOR_DE and w2 in COLOR_DE:
            c1, c2 = COLOR_DE[w1], COLOR_DE[w2]
            if c1 not in found_colors:
                found_colors.append(c1)
            if c2 not in found_colors:
                found_colors.append(c2)

    return found_colors


def mcp_init_session() -> str:
    proc = subprocess.run(
        ["curl", "-s", "-D", "/tmp/mcp_desc_h.txt", "-X", "POST", MCP_URL,
         "-H", f"Authorization: {MCP_AUTH}", "-H", "Content-Type: application/json",
         "-d", json.dumps({"jsonrpc": "2.0", "method": "initialize", "id": 0,
                           "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                                      "clientInfo": {"name": "desc-resolver", "version": "1.0"}}})],
        capture_output=True, text=True, timeout=15)
    with open("/tmp/mcp_desc_h.txt") as f:
        for line in f:
            if line.lower().startswith("mcp-session-id:"):
                return line.split(":", 1)[1].strip()
    raise RuntimeError("No session ID")


def fetch_description(sku: str, session: str) -> str | None:
    """Fetch product description text. Returns HTML string or None."""
    cache_file = CACHE / f"{sku.replace('/', '_')}.txt"
    if cache_file.exists():
        text = cache_file.read_text(encoding="utf-8")
        return text if text.strip() else None

    payload = json.dumps({
        "jsonrpc": "2.0", "method": "tools/call", "id": 1,
        "params": {"name": "getProductDescription",
                   "arguments": {"sku": sku, "typeKey": "general", "locale": "de"}}
    })
    try:
        proc = subprocess.run(
            ["curl", "-s", "-w", "\n%{http_code}", "-X", "POST", MCP_URL,
             "-H", f"Authorization: {MCP_AUTH}", "-H", "Content-Type: application/json",
             "-H", f"Mcp-Session-Id: {session}", "-d", payload],
            capture_output=True, text=True, timeout=15)
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
        content = resp["result"]["content"][0]["text"]
        desc = json.loads(content)
        text = desc.get("text", "")
    except (json.JSONDecodeError, KeyError, IndexError):
        text = ""

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(text, encoding="utf-8")
    return text if text.strip() else None


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)

    # Load medium-confidence SKUs
    work = []
    with (ROOT / "gk_gloves_enriched.csv").open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["Color_confidence"] == "medium" and r["Resolution_source"] in ("mcp", "mcp_rehab"):
                work.append((r["ParentSKU"], r["Marke"], r["Basisfarbe"],
                             float(r["Revenue_total"]), r["ProductName"]))

    work.sort(key=lambda x: -x[3])
    print(f"Medium-confidence SKUs to check: {len(work)}")
    cached = sum(1 for s, *_ in work if (CACHE / f"{s.replace('/','_')}.txt").exists())
    print(f"  Cached: {cached}, To fetch: {len(work) - cached}")

    print("Initializing MCP session...")
    session = mcp_init_session()

    results = []
    n_found = 0
    n_empty = 0
    n_no_color = 0
    t0 = time.time()

    for i, (sku, marke, mcp_basis, rev, name) in enumerate(work):
        if i and i % 100 == 0:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            print(f"  [{i}/{len(work)}]  found={n_found} empty={n_empty} no_color={n_no_color}  "
                  f"{rate:.1f}/s  ETA {(len(work)-i)/rate/60:.1f} min")

        html = fetch_description(sku, session)
        if not html:
            n_empty += 1
            results.append({"ParentSKU": sku, "Marke": marke, "MCP_Basisfarbe": mcp_basis,
                            "Desc_Basisfarbe": "", "Desc_colors": "", "Revenue": rev,
                            "Status": "no_description"})
            continue

        colors = extract_colors_from_text(html)
        if colors:
            n_found += 1
            results.append({"ParentSKU": sku, "Marke": marke, "MCP_Basisfarbe": mcp_basis,
                            "Desc_Basisfarbe": colors[0], "Desc_colors": "/".join(colors),
                            "Revenue": rev, "Status": "found"})
        else:
            n_no_color += 1
            results.append({"ParentSKU": sku, "Marke": marke, "MCP_Basisfarbe": mcp_basis,
                            "Desc_Basisfarbe": "", "Desc_colors": "", "Revenue": rev,
                            "Status": "no_color_in_text"})

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} min")
    print(f"  Found color in text: {n_found}")
    print(f"  No description:      {n_empty}")
    print(f"  Description but no color: {n_no_color}")

    # How many differ from MCP?
    differs = [r for r in results if r["Status"] == "found" and r["Desc_Basisfarbe"] != r["MCP_Basisfarbe"]]
    matches = [r for r in results if r["Status"] == "found" and r["Desc_Basisfarbe"] == r["MCP_Basisfarbe"]]
    print(f"\n  Of {n_found} found: {len(matches)} match MCP, {len(differs)} DIFFER from MCP")
    print(f"  Revenue covered by differs: EUR {sum(r['Revenue'] for r in differs):,.0f}")

    if differs:
        print(f"\n  Top 15 corrections (desc ≠ MCP):")
        for r in sorted(differs, key=lambda x: -x["Revenue"])[:15]:
            print(f"    {r['Marke']:<12} {r['ParentSKU']:<30} MCP={r['MCP_Basisfarbe']:<8} "
                  f"Desc={r['Desc_Basisfarbe']:<8} ({r['Desc_colors']})  EUR {r['Revenue']:>8,.0f}")

    fields = ["ParentSKU", "Marke", "MCP_Basisfarbe", "Desc_Basisfarbe", "Desc_colors", "Revenue", "Status"]
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
