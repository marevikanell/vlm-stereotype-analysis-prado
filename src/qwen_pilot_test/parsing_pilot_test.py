"""
Caption Parser: Qwen2.5-VL Output → Figure-Level Table
========================================================
Parses the structured "Figure N: [Gender]" format from pilot_captions.json
into a figure-level CSV ready for SADCAT scoring.

Usage:
    python parse_captions.py [--input caption_outputs/pilot_captions.json]

Output:
    caption_outputs/pilot_figures.csv
    caption_outputs/parse_report.txt
"""

import json
import re
import csv
import os
import argparse
from collections import Counter

# ── CONFIG ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, default="caption_outputs/pilot_captions.json")
args = parser.parse_args()

OUTPUT_DIR = os.path.dirname(args.input)
figures_csv_path = os.path.join(OUTPUT_DIR, "pilot_figures.csv")
report_path = os.path.join(OUTPUT_DIR, "parse_report.txt")

# ── LOAD DATA ───────────────────────────────────────────────────────
with open(args.input, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Loaded {len(data)} artworks from {args.input}")

# ── PARSE FIGURES ───────────────────────────────────────────────────
# Pattern: "Figure N: Gender" followed by paragraph text
FIGURE_PATTERN = re.compile(
    r'Figure\s+(\d+)\s*:\s*(Male|Female)(?:\s*\(([^)]*)\))?\s*\n(.*?)(?=\nFigure\s+\d+\s*:|$)',
    re.DOTALL | re.IGNORECASE
)

# Also catch "no figure" / "no people" type responses
NO_FIGURE_PATTERNS = [
    r'no\s+(?:main\s+)?figures?\s+(?:are\s+)?visible',
    r'no\s+(?:main\s+)?people',
    r'no\s+human',
    r'there\s+are\s+no\s+(?:other\s+)?(?:main\s+)?figures?',
    r'the\s+painting\s+does\s+not\s+(?:contain|depict|show)',
    r'no\s+figures?\s+(?:can\s+be|are)\s+(?:seen|identified|found)',
]

all_figures = []
parse_stats = {
    "total_artworks": len(data),
    "artworks_with_figures": 0,
    "artworks_no_figures": 0,
    "artworks_failed": 0,
    "artworks_no_caption": 0,
    "total_figures": 0,
    "gender_counts": Counter(),
    "figures_per_artwork": [],
    "context_counts": Counter(),
}

for artwork in data:
    cat_no = artwork["cat_no"]
    context = artwork.get("context", "unknown")
    caption = artwork.get("caption")

    parse_stats["context_counts"][context] += 1

    # Handle failed/missing captions
    if not caption:
        parse_stats["artworks_no_caption"] += 1
        continue

    # Find all figures
    matches = FIGURE_PATTERN.findall(caption)

    if matches:
        parse_stats["artworks_with_figures"] += 1
        parse_stats["figures_per_artwork"].append(len(matches))

        for fig_num, gender, qualifier, paragraph in matches:
            gender_clean = gender.strip().capitalize()
            paragraph_clean = paragraph.strip()

            # Remove trailing "There are no other figures..." lines
            for pattern in NO_FIGURE_PATTERNS:
                paragraph_clean = re.sub(pattern + r'.*$', '', paragraph_clean,
                                         flags=re.IGNORECASE | re.DOTALL).strip()

            # Remove trailing summary/list blocks that Qwen sometimes appends
            TRAILING_JUNK = [
                r'\n\s*All\s+figures\s+visible.*$',
                r'\n\s*Other\s+figures\s*:.*$',
                r'\n\s*List\s+of\s+figures.*$',
                r'\n\s*Additional\s+figures.*$',
                r'\n\s*Background\s+figures.*$',
                r'\n\s*-\s+A\s+group\s+of.*$',
                r'\n\s*-\s+Figure\s+\d+.*$',
                r'\n\s*\d+\.\s+(Male|Female).*$',
            ]
            for pattern in TRAILING_JUNK:
                paragraph_clean = re.sub(pattern, '', paragraph_clean,
                                         flags=re.IGNORECASE | re.DOTALL).strip()

            # Remove markdown bold markers
            paragraph_clean = paragraph_clean.replace("**", "")

            # Skip empty paragraphs
            if not paragraph_clean:
                continue

            figure = {
                "cat_no": cat_no,
                "context": context,
                "figure_num": int(fig_num),
                "gender": gender_clean,
                "qualifier": qualifier.strip() if qualifier else "",
                "descriptor_text": paragraph_clean,
                "descriptor_length": len(paragraph_clean),
                "word_count": len(paragraph_clean.split()),
            }
            all_figures.append(figure)
            parse_stats["total_figures"] += 1
            parse_stats["gender_counts"][gender_clean] += 1

    else:
        # Check if it's a "no figures" response
        is_no_figure = any(
            re.search(p, caption, re.IGNORECASE)
            for p in NO_FIGURE_PATTERNS
        )
        if is_no_figure:
            parse_stats["artworks_no_figures"] += 1
            parse_stats["figures_per_artwork"].append(0)
        else:
            # Could not parse — flag for review
            parse_stats["artworks_failed"] += 1
            print(f"  WARNING: Could not parse {cat_no}. Preview: {caption[:100]}...")

# ── SAVE FIGURES CSV ────────────────────────────────────────────────
fieldnames = ["cat_no", "context", "figure_num", "gender", "qualifier",
              "descriptor_text", "descriptor_length", "word_count"]

with open(figures_csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_figures)

# ── PRINT & SAVE REPORT ────────────────────────────────────────────
report_lines = []

def rprint(text=""):
    print(text)
    report_lines.append(text)

rprint("=" * 60)
rprint("PARSE REPORT")
rprint("=" * 60)
rprint(f"  Input:              {args.input}")
rprint(f"  Total artworks:     {parse_stats['total_artworks']}")
rprint(f"  With figures:       {parse_stats['artworks_with_figures']}")
rprint(f"  No figures found:   {parse_stats['artworks_no_figures']}")
rprint(f"  No caption (error): {parse_stats['artworks_no_caption']}")
rprint(f"  Parse failed:       {parse_stats['artworks_failed']}")
rprint()
rprint(f"  Total figures extracted: {parse_stats['total_figures']}")
rprint(f"  Gender breakdown:")
for gender, count in parse_stats["gender_counts"].most_common():
    rprint(f"    {gender}: {count}")
rprint()

if parse_stats["figures_per_artwork"]:
    avg_figs = sum(parse_stats["figures_per_artwork"]) / len(parse_stats["figures_per_artwork"])
    max_figs = max(parse_stats["figures_per_artwork"])
    rprint(f"  Avg figures/artwork: {avg_figs:.1f}")
    rprint(f"  Max figures/artwork: {max_figs}")

if all_figures:
    avg_words = sum(f["word_count"] for f in all_figures) / len(all_figures)
    rprint(f"  Avg words/figure:    {avg_words:.0f}")

rprint()
rprint(f"  Context breakdown:")
for ctx in ["religious", "secular", "unknown"]:
    ctx_figs = [f for f in all_figures if f["context"] == ctx]
    if ctx_figs:
        ctx_male = sum(1 for f in ctx_figs if f["gender"] == "Male")
        ctx_female = sum(1 for f in ctx_figs if f["gender"] == "Female")
        rprint(f"    {ctx}: {len(ctx_figs)} figures ({ctx_male} M, {ctx_female} F)")

rprint()
rprint(f"  Output: {figures_csv_path}")
rprint(f"  Next step: feed descriptor_text column through SADCAT for scoring.")

# Save report
with open(report_path, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

rprint(f"  Report saved to: {report_path}")