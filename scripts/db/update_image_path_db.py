#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_image_paths.py
─────────────────────
Updates the file_path column in artwork_image table to point
to the new image directory.

Usage:
    python scripts/db/update_image_paths.py \
        --db-path /home/agrupa-lab/agrupa/agrupa.sqlite \
        --new-dir /home/agrupa-lab/agrupa/data_raw/obras
"""

from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(
        description="Update image paths in artwork_image table."
    )
    ap.add_argument("--db-path", type=str, required=True)
    ap.add_argument("--new-dir", type=str, required=True)
    args = ap.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    new_dir = Path(args.new_dir).expanduser().resolve()

    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    if not new_dir.is_dir():
        raise SystemExit(f"New image directory not found: {new_dir}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Get current rows
    cur.execute("SELECT cat_no, file_name, file_path FROM artwork_image")
    rows = cur.fetchall()
    print(f"Total rows in artwork_image: {len(rows)}")

    # Update each path
    updated = 0
    missing = 0
    already_correct = 0

    for cat_no, file_name, old_path in rows:
        new_path = str(new_dir / file_name)

        if old_path == new_path:
            already_correct += 1
            continue

        if Path(new_path).exists():
            cur.execute(
                "UPDATE artwork_image SET file_path = ? WHERE cat_no = ?",
                (new_path, cat_no)
            )
            updated += 1
        else:
            missing += 1
            if missing <= 10:
                print(f"  WARNING: file not found at new path: {new_path}")

    conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print("IMAGE PATH UPDATE REPORT")
    print(f"{'='*50}")
    print(f"  Total rows:        {len(rows)}")
    print(f"  Updated:           {updated}")
    print(f"  Already correct:   {already_correct}")
    print(f"  Missing at new path: {missing}")
    print(f"  New directory:     {new_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
