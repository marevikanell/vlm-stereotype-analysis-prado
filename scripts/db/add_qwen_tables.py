#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_qwen_tables.py
───────────────────
Adds two tables to the AGRUPA SQLite database:

1. qwen_captions_m  — raw Qwen2.5-VL captions per artwork
2. figures_m         — parsed figures with SADCAT scores per figure

Both link to artwork.cat_no as foreign key.

"""

from __future__ import annotations
import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd


# ── Schema ──────────────────────────────────────────────────────────
DDL = """
PRAGMA foreign_keys = ON;

-- Raw captions: one row per artwork
CREATE TABLE IF NOT EXISTS qwen_captions_m (
    cat_no          TEXT PRIMARY KEY,
    context         TEXT,
    caption         TEXT,
    inference_time_s REAL,
    caption_length  INTEGER,
    num_figures     INTEGER,
    model_id        TEXT DEFAULT 'Qwen/Qwen2.5-VL-7B-Instruct',
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(cat_no) REFERENCES artwork(cat_no)
);

-- Parsed figures with SADCAT scores: one row per figure
CREATE TABLE IF NOT EXISTS figures_m (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cat_no          TEXT NOT NULL,
    context         TEXT,
    figure_num      INTEGER NOT NULL,
    gender          TEXT,
    qualifier       TEXT,
    descriptor_text TEXT,
    descriptor_length INTEGER,
    word_count      INTEGER,
    n_descriptors   INTEGER,
    n_in_dict       INTEGER,
    coverage_pct    REAL,
    dirmean_Warmth  REAL,
    dirmean_Competence REAL,
    dirmean_Sociability REAL,
    dirmean_Morality REAL,
    dirmean_Ability REAL,
    dirmean_Assertiveness REAL,
    dirmean_Status  REAL,
    dirmean_Beliefs REAL,
    dirmean_health  REAL,
    dirmean_deviance REAL,
    dirmean_beauty  REAL,
    dirmean_Politics REAL,
    dirmean_Religion REAL,
    n_dirmean_Warmth INTEGER,
    n_dirmean_Competence INTEGER,
    n_dirmean_Sociability INTEGER,
    n_dirmean_Morality INTEGER,
    n_dirmean_Ability INTEGER,
    n_dirmean_Assertiveness INTEGER,
    n_dirmean_Status INTEGER,
    n_dirmean_Beliefs INTEGER,
    n_dirmean_health INTEGER,
    n_dirmean_deviance INTEGER,
    n_dirmean_beauty INTEGER,
    n_dirmean_Politics INTEGER,
    n_dirmean_Religion INTEGER,
    asymmetry       REAL,
    model_id        TEXT DEFAULT 'Qwen/Qwen2.5-VL-7B-Instruct',
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(cat_no) REFERENCES artwork(cat_no)
);

CREATE INDEX IF NOT EXISTS idx_figures_m_cat ON figures_m(cat_no);
CREATE INDEX IF NOT EXISTS idx_figures_m_gender ON figures_m(gender);
CREATE INDEX IF NOT EXISTS idx_figures_m_context ON figures_m(context);
"""


def main():
    ap = argparse.ArgumentParser(
        description="Add Qwen caption and figure score tables to AGRUPA SQLite database."
    )
    ap.add_argument("--db-path", type=str, required=True,
                    help="Path to the existing SQLite database")
    ap.add_argument("--captions", type=str, required=True,
                    help="Path to batch_captions.json")
    ap.add_argument("--figures", type=str, required=True,
                    help="Path to batch_figures.csv")
    ap.add_argument("--scores", type=str, required=True,
                    help="Path to batch_sadcat_scores.csv")
    args = ap.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    # ── Connect & create tables ─────────────────────────────────────
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL)
    cur = conn.cursor()

    # ── Load existing artwork cat_nos for validation ────────────────
    cur.execute("SELECT cat_no FROM artwork")
    db_cat_nos = {row[0] for row in cur.fetchall()}
    print(f"Artworks in DB: {len(db_cat_nos)}")

    # ════════════════════════════════════════════════════════════════
    # 1. QWEN CAPTIONS
    # ════════════════════════════════════════════════════════════════
    print(f"\nLoading captions from {args.captions}...")
    with open(args.captions, "r", encoding="utf-8") as f:
        captions_data = json.load(f)

    # Clear existing data
    cur.execute("DELETE FROM qwen_captions_m")

    insert_caption_sql = """
    INSERT OR REPLACE INTO qwen_captions_m
        (cat_no, context, caption, inference_time_s, caption_length, num_figures)
    VALUES (?, ?, ?, ?, ?, ?)
    """

    caption_rows = []
    skipped_captions = 0
    for r in captions_data:
        cat_no = r.get("cat_no")
        if cat_no not in db_cat_nos:
            skipped_captions += 1
            continue
        caption_rows.append((
            cat_no,
            r.get("context"),
            r.get("caption"),
            r.get("inference_time_s"),
            r.get("caption_length"),
            r.get("num_figures"),
        ))

    with conn:
        cur.executemany(insert_caption_sql, caption_rows)

    print(f"  Inserted: {len(caption_rows)} captions")
    if skipped_captions > 0:
        print(f"  Skipped (no matching artwork): {skipped_captions}")

    # ════════════════════════════════════════════════════════════════
    # 2. FIGURES WITH SADCAT SCORES
    # ════════════════════════════════════════════════════════════════
    print(f"\nLoading figures from {args.figures}...")
    figures_df = pd.read_csv(args.figures, dtype={"cat_no": str})
    print(f"  Figures CSV rows: {len(figures_df)}")

    print(f"Loading SADCAT scores from {args.scores}...")
    scores_df = pd.read_csv(args.scores, dtype={"cat_no": str})
    print(f"  Scores CSV rows: {len(scores_df)}")

    # Merge figures with scores on cat_no + figure_num + gender
    # Figures CSV has: cat_no, context, figure_num, gender, qualifier, descriptor_text, descriptor_length, word_count
    # Scores CSV has: cat_no, context, figure_num, gender, qualifier, n_descriptors, n_in_dict, coverage_pct, dirmean_*, n_dirmean_*, asymmetry
    # We merge to get descriptor_text from figures + scores from scores

    merged = pd.merge(
        figures_df[["cat_no", "context", "figure_num", "gender", "qualifier",
                     "descriptor_text", "descriptor_length", "word_count"]],
        scores_df.drop(columns=["context", "gender", "qualifier"], errors="ignore"),
        on=["cat_no", "figure_num"],
        how="left"
    )

    print(f"  Merged rows: {len(merged)}")

    # Filter to only artworks that exist in DB
    merged = merged[merged["cat_no"].isin(db_cat_nos)]
    print(f"  After filtering to DB artworks: {len(merged)}")

    # Clear existing data
    cur.execute("DELETE FROM figures_m")

    # Build column list matching the table schema
    figure_columns = [
        "cat_no", "context", "figure_num", "gender", "qualifier",
        "descriptor_text", "descriptor_length", "word_count",
        "n_descriptors", "n_in_dict", "coverage_pct",
        "dirmean_Warmth", "dirmean_Competence", "dirmean_Sociability",
        "dirmean_Morality", "dirmean_Ability", "dirmean_Assertiveness",
        "dirmean_Status", "dirmean_Beliefs", "dirmean_health",
        "dirmean_deviance", "dirmean_beauty", "dirmean_Politics",
        "dirmean_Religion",
        "n_dirmean_Warmth", "n_dirmean_Competence", "n_dirmean_Sociability",
        "n_dirmean_Morality", "n_dirmean_Ability", "n_dirmean_Assertiveness",
        "n_dirmean_Status", "n_dirmean_Beliefs", "n_dirmean_health",
        "n_dirmean_deviance", "n_dirmean_beauty", "n_dirmean_Politics",
        "n_dirmean_Religion",
        "asymmetry",
    ]

    # Ensure all columns exist in merged df (fill missing with None)
    for col in figure_columns:
        if col not in merged.columns:
            merged[col] = None

    placeholders = ",".join(["?"] * len(figure_columns))
    col_names = ",".join(figure_columns)
    insert_figure_sql = f"""
    INSERT INTO figures_m ({col_names})
    VALUES ({placeholders})
    """

    figure_rows = []
    for _, row in merged.iterrows():
        values = []
        for col in figure_columns:
            val = row[col]
            # Convert NaN to None for SQLite
            if pd.isna(val):
                values.append(None)
            else:
                values.append(val)
        figure_rows.append(tuple(values))

    with conn:
        cur.executemany(insert_figure_sql, figure_rows)

    print(f"  Inserted: {len(figure_rows)} figures with scores")

    # ════════════════════════════════════════════════════════════════
    # REPORT
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 55)
    print("DATABASE UPDATE REPORT")
    print("=" * 55)

    cur.execute("SELECT COUNT(*) FROM qwen_captions_m")
    n_captions = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM figures_m")
    n_figures = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT cat_no) FROM figures_m")
    n_artworks_with_figures = cur.fetchone()[0]

    cur.execute("SELECT gender, COUNT(*) FROM figures_m GROUP BY gender ORDER BY COUNT(*) DESC")
    gender_rows = cur.fetchall()

    cur.execute("SELECT context, COUNT(*) FROM figures_m GROUP BY context ORDER BY COUNT(*) DESC")
    context_rows = cur.fetchall()

    cur.execute("""
        SELECT context, gender, COUNT(*),
               AVG(dirmean_Warmth), AVG(dirmean_Competence), AVG(asymmetry)
        FROM figures_m
        WHERE gender IN ('Male', 'Female')
        GROUP BY context, gender
        ORDER BY context, gender
    """)
    ctx_gender_rows = cur.fetchall()

    print(f"  qwen_captions_m:  {n_captions} rows")
    print(f"  figures_m:        {n_figures} rows")
    print(f"  Artworks w/ figures: {n_artworks_with_figures}")

    print(f"\n  Gender breakdown:")
    for gender, count in gender_rows:
        print(f"    {gender}: {count}")

    print(f"\n  Context breakdown:")
    for context, count in context_rows:
        print(f"    {context}: {count}")

    print(f"\n  Context × Gender (from DB):")
    for ctx, gen, count, w, c, a in ctx_gender_rows:
        print(f"    {ctx} {gen} (n={count}): W={w:.3f}, C={c:.3f}, A={a:.3f}")

    # Verify foreign key integrity
    cur.execute("""
        SELECT COUNT(*) FROM qwen_captions_m
        WHERE cat_no NOT IN (SELECT cat_no FROM artwork)
    """)
    orphan_captions = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM figures_m
        WHERE cat_no NOT IN (SELECT cat_no FROM artwork)
    """)
    orphan_figures = cur.fetchone()[0]

    print(f"\n  FK integrity:")
    print(f"    Orphan captions (no artwork): {orphan_captions}")
    print(f"    Orphan figures (no artwork):  {orphan_figures}")

    print("=" * 55)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
