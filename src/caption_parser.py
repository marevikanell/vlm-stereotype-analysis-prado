"""
Batch Caption Parser: Qwen2.5-VL Output → Figure-Level CSV
=============================================================
Parses structured "Figure N: [Gender]" format from batch_captions.json
into a figure-level CSV ready for SADCAT scoring.

Handles edge cases:
- "Male**" / "Female**" (markdown artifacts)
- "Male (left)" / "Female (Background)" etc (qualifiers in parens)
- "Male Angel" / "Female Angel" / "Male Child" etc (compound labels)
- "Child" / "Baby" / "Infant" / "Angel" without gender → mapped
- Animals, objects, landscapes → skipped (non-human)
- "No figures" / "still life" / "landscape" responses → logged
- Trailing junk (summaries, lists, background descriptions)

Usage:
    python parse_batch.py [--input caption_outputs/batch_captions.json]

Output:
    caption_outputs/batch_figures.csv
    caption_outputs/batch_parse_report.txt
"""

import json
import re
import csv
import os
import argparse
from collections import Counter

# ── CONFIG ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, default="caption_outputs/batch_captions.json")
args = parser.parse_args()

OUTPUT_DIR = os.path.dirname(args.input)
figures_csv_path = os.path.join(OUTPUT_DIR, "batch_figures.csv")
report_path = os.path.join(OUTPUT_DIR, "batch_parse_report.txt")
skipped_path = os.path.join(OUTPUT_DIR, "batch_skipped.json")

# ── LOAD DATA ───────────────────────────────────────────────────────
with open(args.input, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Loaded {len(data)} artworks from {args.input}")

# ── GENDER MAPPING ──────────────────────────────────────────────────
# Map the raw gender line to a clean gender label
# Returns (gender, qualifier, is_human)

# Non-human labels to skip entirely
NON_HUMAN = {
    'dog', 'horse', 'cow', 'cat', 'bird', 'sheep', 'donkey', 'tree',
    'landscape', 'skull', 'animal', 'table', 'background', 'fish',
    'basket', 'rooster', 'parrot', 'tablecloth', 'swan', 'grapes',
    'rabbit', 'bull', 'dove', 'seashells', 'house', 'bouquet',
    'globe', 'monkey', 'scroll', 'peacock', 'plate', 'flowers',
    'shell', 'chickens', 'figs', 'cattle', 'hen', 'books', 'hawk',
    'vase', 'clock', 'cross', 'inanimate object', 'floral arrangement',
    'hanging birds', 'object', 'book', 'lion', 'lamb', 'deer',
    'pig', 'goat', 'serpent', 'snake', 'eagle', 'ox',
}

def classify_gender(raw_label):
    """
    Returns (gender, qualifier, is_human)
    gender: 'Male', 'Female', 'Unknown'
    qualifier: extra info like 'Angel', 'Child', 'Background', etc.
    is_human: True if this is a human figure worth scoring
    """
    # Clean up markdown artifacts
    label = raw_label.replace('**', '').strip()
    label_lower = label.lower()
    
    # Check if it's a non-human entity
    # Check exact match first
    if label_lower in NON_HUMAN:
        return None, label, False
    
    # Check if it starts with a non-human word
    first_word = label_lower.split()[0] if label_lower else ''
    if first_word in NON_HUMAN:
        return None, label, False
    
    # Check for "Animal (Dog)" pattern
    if label_lower.startswith('animal') or label_lower.startswith('inanimate'):
        return None, label, False
    
    # Male patterns
    if label_lower.startswith('male'):
        qualifier = label[4:].strip().strip('()').strip()
        # "Male Lion" → non-human
        if qualifier.lower() in NON_HUMAN:
            return None, label, False
        return 'Male', qualifier, True
    
    # Female patterns
    if label_lower.startswith('female'):
        qualifier = label[6:].strip().strip('()').strip()
        return 'Female', qualifier, True
    
    # Gender-ambiguous human labels
    if label_lower in ('child', 'child (seated)', 'child (standing)', 'child (baby jesus)'):
        return 'Unknown', 'Child', True
    
    if label_lower in ('baby', 'baby jesus', 'infant', 'infant jesus'):
        return 'Unknown', label, True
    
    if label_lower in ('angel', 'angelic figure', 'angelic figures', 'cherub'):
        return 'Unknown', label, True
    
    if label_lower in ('woman', 'older woman', 'young woman', 'girl'):
        return 'Female', label, True
    
    if label_lower in ('man', 'older man', 'young man', 'boy', 'man in the distance'):
        return 'Male', label, True
    
    if label_lower in ('central figure', 'figure'):
        return 'Unknown', label, True
    
    # Anything else — skip
    return None, label, False


# ── FIGURE PATTERN ──────────────────────────────────────────────────
FIGURE_PATTERN = re.compile(
    r'Figure\s+(\d+)\s*:\s*([^\n]+)\n(.*?)(?=\nFigure\s+\d+\s*:|$)',
    re.DOTALL | re.IGNORECASE
)

# Patterns to strip from the end of paragraphs
TRAILING_JUNK = [
    r'\n\s*All\s+figures\s+visible.*$',
    r'\n\s*Other\s+figures\s*:.*$',
    r'\n\s*List\s+of\s+figures.*$',
    r'\n\s*Additional\s+figures.*$',
    r'\n\s*Background\s+figures.*$',
    r'\n\s*-\s+A\s+group\s+of.*$',
    r'\n\s*-\s+Figure\s+\d+.*$',
    r'\n\s*\d+\.\s+(Male|Female).*$',
    r'\n\s*Note\s*:.*$',
    r'\n\s*The\s+painting\s+does\s+not.*$',
    r'\n\s*There\s+are\s+no\s+other.*$',
    r'\n\s*No\s+other\s+figures.*$',
    r'\n\s*The\s+main\s+elements.*$',
    r'\n\s*\d+\.\s+\*\*.*$',
]

NO_FIGURE_PATTERNS = [
    r'no\s+(?:main\s+)?(?:human\s+)?figures?\s+(?:are\s+)?(?:present|visible)',
    r'no\s+(?:main\s+)?(?:human\s+)?people',
    r'no\s+human\s+figures?\s+(?:present|to\s+describe)',
    r'there\s+are\s+no\s+(?:other\s+)?(?:main\s+)?figures?',
    r'the\s+painting\s+does\s+not\s+(?:contain|depict|show)\s+any\s+human',
    r'no\s+figures?\s+(?:can\s+be|are)\s+(?:seen|identified|found)',
    r'still\s+life\s+painting',
    r'landscape\s+(?:painting|scene)',
    r'not\s+depicting\s+any\s+human',
    r'cannot\s+provide\s+the\s+requested\s+descriptions\s+for\s+human',
    r'not\s+a\s+photograph\s+of\s+real\s+people',
]

# ── PARSE ───────────────────────────────────────────────────────────
all_figures = []
skipped_non_human = []
no_figure_artworks = []

stats = {
    "total_artworks": len(data),
    "artworks_with_figures": 0,
    "artworks_no_figures": 0,
    "artworks_no_caption": 0,
    "artworks_parse_failed": 0,
    "total_figures_found": 0,
    "human_figures_kept": 0,
    "non_human_skipped": 0,
    "gender_counts": Counter(),
    "qualifier_counts": Counter(),
    "context_counts": Counter(),
    "figures_per_artwork": [],
}

for artwork in data:
    cat_no = artwork["cat_no"]
    context = artwork.get("context", "unknown")
    caption = artwork.get("caption")
    
    stats["context_counts"][context] += 1
    
    if not caption:
        stats["artworks_no_caption"] += 1
        continue
    
    # Check if this is a "no figures" response
    is_no_figure = any(
        re.search(p, caption, re.IGNORECASE)
        for p in NO_FIGURE_PATTERNS
    )
    
    # Find figures
    matches = FIGURE_PATTERN.findall(caption)
    
    if not matches:
        if is_no_figure or 'Figure' not in caption:
            stats["artworks_no_figures"] += 1
            no_figure_artworks.append({
                "cat_no": cat_no,
                "context": context,
                "reason": caption[:200],
            })
        else:
            stats["artworks_parse_failed"] += 1
        continue
    
    stats["artworks_with_figures"] += 1
    artwork_figure_count = 0
    
    for fig_num, gender_line, paragraph in matches:
        stats["total_figures_found"] += 1
        
        gender, qualifier, is_human = classify_gender(gender_line.strip())
        
        if not is_human:
            stats["non_human_skipped"] += 1
            skipped_non_human.append({
                "cat_no": cat_no,
                "figure_num": int(fig_num),
                "raw_label": gender_line.strip(),
            })
            continue
        
        # Clean paragraph
        paragraph_clean = paragraph.strip()
        
        # Remove markdown bold
        paragraph_clean = paragraph_clean.replace('**', '')
        
        # Remove trailing junk
        for pattern in TRAILING_JUNK:
            paragraph_clean = re.sub(pattern, '', paragraph_clean,
                                     flags=re.IGNORECASE | re.DOTALL).strip()
        
        # Remove "no other figures" type endings
        for pattern in NO_FIGURE_PATTERNS:
            paragraph_clean = re.sub(r'\n.*' + pattern + r'.*$', '', paragraph_clean,
                                     flags=re.IGNORECASE | re.DOTALL).strip()
        
        # Skip empty paragraphs
        if not paragraph_clean or len(paragraph_clean) < 10:
            continue
        
        figure = {
            "cat_no": cat_no,
            "context": context,
            "figure_num": int(fig_num),
            "gender": gender or "Unknown",
            "qualifier": qualifier if qualifier else "",
            "descriptor_text": paragraph_clean,
            "descriptor_length": len(paragraph_clean),
            "word_count": len(paragraph_clean.split()),
        }
        
        all_figures.append(figure)
        stats["human_figures_kept"] += 1
        stats["gender_counts"][gender or "Unknown"] += 1
        if qualifier:
            stats["qualifier_counts"][qualifier] += 1
        artwork_figure_count += 1
    
    stats["figures_per_artwork"].append(artwork_figure_count)

# ── SAVE FIGURES CSV ────────────────────────────────────────────────
fieldnames = ["cat_no", "context", "figure_num", "gender", "qualifier",
              "descriptor_text", "descriptor_length", "word_count"]

with open(figures_csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_figures)

# ── SAVE SKIPPED NON-HUMAN ──────────────────────────────────────────
with open(skipped_path, "w", encoding="utf-8") as f:
    json.dump({
        "non_human_figures": skipped_non_human,
        "no_figure_artworks": no_figure_artworks,
    }, f, indent=2, ensure_ascii=False)

# ── REPORT ──────────────────────────────────────────────────────────
report_lines = []

def rprint(text=""):
    print(text)
    report_lines.append(text)

rprint("=" * 60)
rprint("BATCH PARSE REPORT")
rprint("=" * 60)
rprint(f"  Input:                {args.input}")
rprint(f"  Total artworks:       {stats['total_artworks']}")
rprint(f"  With human figures:   {stats['artworks_with_figures']}")
rprint(f"  No figures (landscape/still life): {stats['artworks_no_figures']}")
rprint(f"  No caption (error):   {stats['artworks_no_caption']}")
rprint(f"  Parse failed:         {stats['artworks_parse_failed']}")
rprint()
rprint(f"  Total figure entries found: {stats['total_figures_found']}")
rprint(f"  Human figures kept:         {stats['human_figures_kept']}")
rprint(f"  Non-human skipped:          {stats['non_human_skipped']}")
rprint()
rprint(f"  Gender breakdown:")
for gender, count in stats["gender_counts"].most_common():
    pct = count / stats["human_figures_kept"] * 100 if stats["human_figures_kept"] > 0 else 0
    rprint(f"    {gender}: {count} ({pct:.1f}%)")

rprint()
if stats["figures_per_artwork"]:
    avg_figs = sum(stats["figures_per_artwork"]) / len(stats["figures_per_artwork"])
    max_figs = max(stats["figures_per_artwork"])
    rprint(f"  Avg human figures/artwork: {avg_figs:.1f}")
    rprint(f"  Max human figures/artwork: {max_figs}")

if all_figures:
    avg_words = sum(f["word_count"] for f in all_figures) / len(all_figures)
    rprint(f"  Avg words/figure:         {avg_words:.0f}")

rprint()
rprint(f"  Context breakdown:")
for ctx in ["religious", "secular", "unknown"]:
    ctx_figs = [f for f in all_figures if f["context"] == ctx]
    if ctx_figs:
        ctx_male = sum(1 for f in ctx_figs if f["gender"] == "Male")
        ctx_female = sum(1 for f in ctx_figs if f["gender"] == "Female")
        ctx_unknown = sum(1 for f in ctx_figs if f["gender"] == "Unknown")
        rprint(f"    {ctx}: {len(ctx_figs)} figures ({ctx_male} M, {ctx_female} F, {ctx_unknown} Unk)")

rprint()
rprint(f"  Top qualifiers:")
for qual, count in stats["qualifier_counts"].most_common(15):
    rprint(f"    {qual}: {count}")

rprint()
rprint(f"  Non-human entities skipped (top 10):")
non_human_labels = Counter(s["raw_label"] for s in skipped_non_human)
for label, count in non_human_labels.most_common(10):
    rprint(f"    {label}: {count}")

rprint()
rprint(f"  Output files:")
rprint(f"    Figures CSV:     {figures_csv_path}")
rprint(f"    Skipped JSON:    {skipped_path}")
rprint(f"    This report:     {report_path}")
rprint(f"\n  Next: run SADCAT scoring on batch_figures.csv")

with open(report_path, "w", encoding="utf-8") as f:
    f.write("\n".join(report_lines))

rprint(f"\n  Report saved to: {report_path}")