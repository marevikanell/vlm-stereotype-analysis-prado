"""
Diagnostic: Can we link animals to specific figures?

Checks two data sources:
1. VLM descriptor_text (figure-level) — does Qwen mention animals near figures?
2. Museum descripcion (artwork-level) — does the Prado description mention animals 
   in ways that could be parsed to figure-level?

Run this BEFORE deciding whether H3 is feasible at the figure level.
"""

import sqlite3
import pandas as pd
import re
from collections import Counter

DB_PATH = '/home/agrupa-lab/agrupa/agrupa.sqlite'
conn = sqlite3.connect(DB_PATH)

# ════════════════════════════════════════════════════════════════
# 1. VLM descriptor_text: animal mentions at figure level
# ════════════════════════════════════════════════════════════════

print("=" * 70)
print("1. VLM DESCRIPTOR TEXT — Animal mentions in figure descriptions")
print("=" * 70)

figures = pd.read_sql("""
    SELECT f.cat_no, f.figure_num, f.gender, f.descriptor_text,
           a.animal_cluster, a.is_religious
    FROM figures_m f
    JOIN artwork a ON f.cat_no = a.cat_no
    WHERE f.gender IN ('Male', 'Female')
""", conn)

# Animal keywords (English — Qwen outputs English)
ANIMAL_WORDS = {
    'purity': ['lamb', 'lambs', 'sheep', 'dove', 'doves', 'pigeon', 'fish', 'fishes'],
    'power': ['horse', 'horses', 'horseback', 'stallion', 'mare', 'mounted', 'equestrian',
              'eagle', 'eagles', 'lion', 'lions', 'lioness',
              'dragon', 'dragons', 'serpent', 'serpents', 'snake', 'snakes'],
    'other': ['dog', 'dogs', 'hound', 'cat', 'cats', 'deer', 'stag', 'bird', 'birds',
              'parrot', 'owl', 'hawk', 'falcon', 'rabbit', 'hare', 'bull', 'ox', 'cow',
              'monkey', 'ape', 'bear', 'donkey', 'mule', 'goat']
}

all_animal_words = {}
for cluster, words in ANIMAL_WORDS.items():
    for w in words:
        all_animal_words[w] = cluster

# Relational patterns — does the text link the animal TO the figure?
RELATIONAL_PATTERNS = [
    r'(?:holding|carries|carrying|with|beside|near|next to|accompanied by|riding|upon|on)\s+(?:a\s+)?({animals})',
    r'({animals})\s+(?:at|near|beside|by|under|on)\s+(?:her|his|their)',
    r'(?:her|his)\s+({animals})',
    r'(?:seated|sitting|standing|kneeling)\s+(?:on|upon|beside|near|with)\s+(?:a\s+)?({animals})',
    r'(?:a|the)\s+({animals})\s+(?:in|on|at)\s+(?:her|his)',
]

animal_pattern = '|'.join(all_animal_words.keys())

def find_animal_mentions(text):
    """Find animal words and relational patterns in descriptor text."""
    if not isinstance(text, str):
        return [], []
    
    text_lower = text.lower()
    words = re.findall(r'[a-z]+', text_lower)
    
    # Simple mentions
    found = [w for w in words if w in all_animal_words]
    
    # Relational mentions (animal linked to the figure)
    relational = []
    for pattern in RELATIONAL_PATTERNS:
        p = pattern.format(animals=animal_pattern)
        matches = re.findall(p, text_lower)
        relational.extend(matches)
    
    return list(set(found)), relational

figures['animal_mentions'], figures['relational_mentions'] = zip(
    *figures['descriptor_text'].apply(find_animal_mentions)
)

figures['has_any_animal'] = figures['animal_mentions'].apply(len) > 0
figures['has_relational'] = figures['relational_mentions'].apply(len) > 0

total = len(figures)
with_animal = figures['has_any_animal'].sum()
with_relational = figures['has_relational'].sum()

print(f"\nTotal figures (M/F): {total:,}")
print(f"With ANY animal word: {with_animal:,} ({with_animal/total*100:.1f}%)")
print(f"With RELATIONAL animal link: {with_relational:,} ({with_relational/total*100:.1f}%)")

# Show examples
if with_animal > 0:
    print(f"\n── Examples: figures mentioning animals ──")
    examples = figures[figures['has_any_animal']].head(15)
    for _, row in examples.iterrows():
        preview = row['descriptor_text'][:200]
        print(f"\n  [{row['cat_no']}] Fig {row['figure_num']} ({row['gender']}) "
              f"cluster={row['animal_cluster']}")
        print(f"    Animals: {row['animal_mentions']}")
        print(f"    Relational: {row['relational_mentions']}")
        print(f"    \"{preview}...\"")

if with_relational > 0:
    print(f"\n── Examples: RELATIONAL animal-figure links ──")
    examples_rel = figures[figures['has_relational']].head(15)
    for _, row in examples_rel.iterrows():
        preview = row['descriptor_text'][:200]
        print(f"\n  [{row['cat_no']}] Fig {row['figure_num']} ({row['gender']})")
        print(f"    Relational: {row['relational_mentions']}")
        print(f"    \"{preview}...\"")

# Word frequency
all_words = []
for word_list in figures['animal_mentions']:
    all_words.extend(word_list)
if all_words:
    print(f"\n── Animal word frequency in VLM descriptions ──")
    for word, count in Counter(all_words).most_common(20):
        cluster = all_animal_words[word]
        print(f"  {word:15s} ({cluster:7s}): {count:,} figures")


# ════════════════════════════════════════════════════════════════
# 2. Museum descripcion: animal mentions at artwork level
# ════════════════════════════════════════════════════════════════

print("\n\n" + "=" * 70)
print("2. MUSEUM DESCRIPCION — Animal mentions in Prado text")
print("=" * 70)

# Check if descripcion column exists
try:
    museum = pd.read_sql("""
        SELECT a.cat_no, a.descripcion, a.animal_cluster, a.is_religious
        FROM artwork a
        WHERE a.descripcion IS NOT NULL AND a.descripcion != ''
    """, conn)
    
    print(f"\nArtworks with museum description: {len(museum):,}")
    
    # Check language — is it Spanish or English?
    sample_texts = museum['descripcion'].head(5).tolist()
    print(f"\n── Sample descriptions (first 200 chars) ──")
    for i, t in enumerate(sample_texts):
        print(f"  [{i}]: {t[:200]}...")
    
    # Spanish + English animal keywords for museum text
    MUSEUM_ANIMAL_WORDS = {
        'purity_es': ['cordero', 'oveja', 'paloma', 'pez', 'peces'],
        'purity_en': ['lamb', 'sheep', 'dove', 'fish'],
        'power_es': ['caballo', 'águila', 'aguila', 'león', 'leon', 'dragón', 'dragon', 'serpiente'],
        'power_en': ['horse', 'eagle', 'lion', 'dragon', 'serpent', 'snake'],
        'other_es': ['perro', 'gato', 'ciervo', 'toro', 'buey', 'vaca', 'mono', 'conejo'],
        'other_en': ['dog', 'cat', 'deer', 'bull', 'ox', 'cow', 'monkey', 'rabbit'],
    }
    
    all_museum_words = {}
    for cluster, words in MUSEUM_ANIMAL_WORDS.items():
        base_cluster = cluster.split('_')[0]
        for w in words:
            all_museum_words[w] = base_cluster
    
    def find_museum_animals(text):
        if not isinstance(text, str):
            return []
        words = re.findall(r'[a-záéíóúñü]+', text.lower())
        return list(set(w for w in words if w in all_museum_words))
    
    museum['animal_words'] = museum['descripcion'].apply(find_museum_animals)
    museum['has_animal'] = museum['animal_words'].apply(len) > 0
    
    with_animal_museum = museum['has_animal'].sum()
    print(f"\nArtworks with animal mention in museum text: {with_animal_museum:,} "
          f"({with_animal_museum/len(museum)*100:.1f}%)")
    
    # Cross-reference: do museum descriptions mention animals that match the tags?
    museum_with_tags = museum[museum['animal_cluster'].notna() & (museum['animal_cluster'] != 'none')]
    if len(museum_with_tags) > 0:
        match_rate = museum_with_tags['has_animal'].mean()
        print(f"Among fauna-tagged artworks: {match_rate*100:.1f}% also mention animals in museum text")
    
    # Show examples
    if with_animal_museum > 0:
        print(f"\n── Examples: museum descriptions mentioning animals ──")
        examples_m = museum[museum['has_animal']].head(10)
        for _, row in examples_m.iterrows():
            preview = row['descripcion'][:300]
            print(f"\n  [{row['cat_no']}] cluster={row['animal_cluster']}")
            print(f"    Animals found: {row['animal_words']}")
            print(f"    \"{preview}...\"")
    
    # Word frequency
    all_museum = []
    for wl in museum['animal_words']:
        all_museum.extend(wl)
    if all_museum:
        print(f"\n── Animal word frequency in museum descriptions ──")
        for word, count in Counter(all_museum).most_common(20):
            cluster = all_museum_words[word]
            print(f"  {word:15s} ({cluster:7s}): {count:,} artworks")

except Exception as e:
    print(f"\n⚠️  Could not read museum descriptions: {e}")
    print("Check if 'descripcion' column exists in artwork table.")


# ════════════════════════════════════════════════════════════════
# 3. Summary: what's feasible?
# ════════════════════════════════════════════════════════════════

print("\n\n" + "=" * 70)
print("3. FEASIBILITY SUMMARY")
print("=" * 70)

print(f"""
VLM descriptors:
  - Figures with any animal mention: {with_animal:,} / {total:,} ({with_animal/total*100:.1f}%)
  - Figures with relational link:    {with_relational:,} / {total:,} ({with_relational/total*100:.1f}%)
  
If <2% have relational mentions → figure-level animal linking via VLM is NOT viable.
If >5% have relational mentions → worth exploring as a figure-level moderator.
""")

conn.close()
print("Done.")