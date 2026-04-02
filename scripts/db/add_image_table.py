#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_image_table.py
──────────────────
Scans the image directory, creates the artwork_image table in the
existing SQLite database, and populates it by matching filenames
to artwork.cat_no.

Usage (from ~/agrupa with venv activated):
    python scripts/add_image_table.py \
        --db-path /path/to/agrupa.db \
        --image-dir /home/agrupa-lab/agrupa/scripts/urls

Produces a short coverage report at the end.
"""

from __future__ import annotations
import argparse
import os
import sqlite3
from pathlib import Path
from typing import List, Tuple


# ── Schema ──────────────────────────────────────────────────────────
DDL_IMAGE = """
CREATE TABLE IF NOT EXISTS artwork_image (
    cat_no      TEXT PRIMARY KEY,
    file_name   TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    extension   TEXT NOT NULL,
    file_size_kb REAL,
    FOREIGN KEY(cat_no) REFERENCES artwork(cat_no)
);

CREATE INDEX IF NOT EXISTS idx_artwork_image_cat ON artwork_image(cat_no);
"""


def scan_images(image_dir: Path) -> List[Tuple[str, str, str, str, float]]:
    """
    Walk the image directory and return a list of tuples:
        (cat_no, file_name, file_path, extension, file_size_kb)

    Assumes filenames follow the pattern: <catalogue_number>.jpg
    e.g.  P000001.jpg  →  cat_no = "P000001"
    """
    rows = []
    valid_exts = {".jpg", ".jpeg", ".png", ".webp"}

    for entry in sorted(image_dir.iterdir()):
        if not entry.is_file():
            continue
        ext = entry.suffix.lower()
        if ext not in valid_exts:
            continue

        cat_no = entry.stem.strip()          # filename without extension
        file_name = entry.name
        file_path = str(entry.resolve())
        file_size_kb = round(entry.stat().st_size / 1024, 2)

        rows.append((cat_no, file_name, file_path, ext, file_size_kb))

    return rows


def main():
    ap = argparse.ArgumentParser(
        description="Add artwork_image table to the AGRUPA SQLite database."
    )
    ap.add_argument(
        "--db-path", type=str, required=True,
        help="Path to the existing SQLite database",
    )
    ap.add_argument(
        "--image-dir", type=str, required=True,
        help="Path to the folder with artwork images (e.g. /home/agrupa-lab/agrupa/scripts/urls)",
    )
    args = ap.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    image_dir = Path(args.image_dir).expanduser().resolve()

    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    if not image_dir.is_dir():
        raise SystemExit(f"Image directory not found: {image_dir}")

    # ── Connect & create table ──────────────────────────────────────
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL_IMAGE)
    cur = conn.cursor()

    # ── Scan images ─────────────────────────────────────────────────
    image_rows = scan_images(image_dir)
    print(f"Images found on disk: {len(image_rows)}")

    if not image_rows:
        raise SystemExit("No image files found. Check --image-dir path.")

    # ── Load existing artwork cat_no set ────────────────────────────
    cur.execute("SELECT cat_no FROM artwork")
    db_cat_nos = {row[0] for row in cur.fetchall()}
    print(f"Artworks in DB: {len(db_cat_nos)}")

    # ── Classify: matched / orphan / missing ────────────────────────
    matched = []
    orphan_images = []

    for row in image_rows:
        cat_no = row[0]
        if cat_no in db_cat_nos:
            matched.append(row)
        else:
            orphan_images.append(row)

    image_cat_nos = {r[0] for r in image_rows}
    missing_images = db_cat_nos - image_cat_nos

    # ── Insert matched rows ─────────────────────────────────────────
    insert_sql = """
    INSERT OR REPLACE INTO artwork_image(cat_no, file_name, file_path, extension, file_size_kb)
    VALUES (?, ?, ?, ?, ?)
    """
    with conn:
        cur.executemany(insert_sql, matched)

    # ── Zero-byte check ─────────────────────────────────────────────
    zero_byte = [r for r in matched if r[4] == 0.0]

    # ── Coverage report ─────────────────────────────────────────────
    print("\n" + "=" * 50)
    print("IMAGE INGESTION REPORT")
    print("=" * 50)
    print(f"  Images on disk          : {len(image_rows)}")
    print(f"  Artworks in DB          : {len(db_cat_nos)}")
    print(f"  ✅ Matched (inserted)   : {len(matched)}")
    print(f"  ⚠️  Orphan images        : {len(orphan_images)}")
    print(f"     (image exists, no DB artwork)")
    print(f"  ❌ Missing images        : {len(missing_images)}")
    print(f"     (artwork exists, no image file)")
    if zero_byte:
        print(f"  🔴 Zero-byte files       : {len(zero_byte)}")

    # ── Print samples for debugging ─────────────────────────────────
    if orphan_images:
        sample = orphan_images[:10]
        print(f"\n  Sample orphan filenames (up to 10):")
        for r in sample:
            print(f"    {r[1]}")

    if missing_images:
        sample = sorted(missing_images)[:10]
        print(f"\n  Sample missing cat_nos (up to 10):")
        for cn in sample:
            print(f"    {cn}")

    print("=" * 50)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
