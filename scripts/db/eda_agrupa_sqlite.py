#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime
import textwrap
import re

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_sql(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def save_md_table(df: pd.DataFrame, path: Path, title: str | None = None) -> None:
    # requiere tabulate (instalado en el bloque)
    md = []
    if title:
        md.append(f"## {title}\n")
    md.append(df.to_markdown(index=False))
    md.append("\n")
    path.write_text("\n".join(md), encoding="utf-8")


def plot_bar(df: pd.DataFrame, x: str, y: str, title: str, path: Path, rotation: int = 60, top: int | None = None):
    d = df.copy()
    if top is not None:
        d = d.head(top)
    plt.figure(figsize=(12, 6))
    plt.bar(d[x].astype(str), d[y].astype(float))
    plt.title(title)
    plt.ylabel(y)
    plt.xticks(rotation=rotation, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def plot_hist(values: np.ndarray, bins: int, title: str, xlabel: str, path: Path):
    plt.figure(figsize=(10, 6))
    plt.hist(values, bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("Frecuencia")
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=str, default="~/agrupa/agrupa.sqlite", help="Ruta a agrupa.sqlite")
    ap.add_argument("--out", type=str, default="~/agrupa/eda_outputs", help="Carpeta base de salida")
    ap.add_argument("--topk", type=int, default=30, help="Top-K tags en gráficos/tablas")
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    out_base = Path(args.out).expanduser().resolve()
    topk = int(args.topk)

    if not db_path.exists():
        raise SystemExit(f"No existe DB en: {db_path}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = out_base / ts
    tables_dir = out_dir / "tables"
    figs_dir = out_dir / "figures"
    ensure_dir(tables_dir)
    ensure_dir(figs_dir)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")

    # ---------- 1) Tamaños básicos ----------
    sizes = read_sql(conn, """
        SELECT 'artwork' AS table_name, COUNT(*) AS n FROM artwork
        UNION ALL SELECT 'icon_tag', COUNT(*) FROM icon_tag
        UNION ALL SELECT 'artwork_tag_raw', COUNT(*) FROM artwork_tag_raw
        UNION ALL SELECT 'artwork_tag', COUNT(*) FROM artwork_tag
        UNION ALL SELECT 'tag_closure', COUNT(*) FROM tag_closure;
    """)
    save_csv(sizes, tables_dir / "01_table_sizes.csv")

    # ---------- 2) Raíces ----------
    roots = read_sql(conn, """
        SELECT tag_id, name, slug
        FROM icon_tag
        WHERE parent_id IS NULL
        ORDER BY slug;
    """)
    save_csv(roots, tables_dir / "02_roots.csv")

    # ---------- 3) Conteos por rama raíz (usa closure + minimizado) ----------
    root_counts = []
    for _, r in roots.iterrows():
        rid = int(r["tag_id"])
        slug = str(r["slug"])
        n = read_sql(conn, """
            SELECT COUNT(DISTINCT at.cat_no) AS n_artworks
            FROM artwork_tag at
            JOIN tag_closure tc ON tc.descendant_id = at.tag_id
            WHERE tc.ancestor_id = ?
        """, (rid,)).iloc[0, 0]
        root_counts.append({"root_slug": slug, "root_id": rid, "n_artworks": int(n)})

    root_counts_df = pd.DataFrame(root_counts).sort_values("n_artworks", ascending=False)
    save_csv(root_counts_df, tables_dir / "03_root_counts.csv")
    plot_bar(root_counts_df, "root_slug", "n_artworks",
             "Número de obras por raíz (usando cierre transitivo)", figs_dir / "03_root_counts.png", rotation=20)

    # ---------- 4) Top tags globales (minimizado) ----------
    top_tags = read_sql(conn, f"""
        SELECT it.slug, it.name, it.depth, COUNT(DISTINCT at.cat_no) AS n_artworks
        FROM artwork_tag at
        JOIN icon_tag it ON it.tag_id = at.tag_id
        GROUP BY it.slug, it.name, it.depth
        ORDER BY n_artworks DESC
        LIMIT {topk};
    """)
    save_csv(top_tags, tables_dir / "04_top_tags_overall.csv")
    plot_bar(top_tags, "slug", "n_artworks",
             f"Top {topk} tags (minimizado)", figs_dir / "04_top_tags_overall.png", rotation=75)

    # ---------- 5) Distribución de profundidad (taxonomía y asignaciones) ----------
    depth_tax = read_sql(conn, """
        SELECT depth, COUNT(*) AS n_tags
        FROM icon_tag
        GROUP BY depth
        ORDER BY depth;
    """)
    save_csv(depth_tax, tables_dir / "05_depth_taxonomy.csv")

    depth_assign = read_sql(conn, """
        SELECT it.depth, COUNT(*) AS n_links
        FROM artwork_tag at
        JOIN icon_tag it ON it.tag_id = at.tag_id
        GROUP BY it.depth
        ORDER BY it.depth;
    """)
    save_csv(depth_assign, tables_dir / "05_depth_assignments.csv")

    plot_bar(depth_tax, "depth", "n_tags",
             "Número de tags por profundidad (taxonomía)", figs_dir / "05_depth_taxonomy.png", rotation=0)
    plot_bar(depth_assign, "depth", "n_links",
             "Número de enlaces obra-tag por profundidad (minimizado)", figs_dir / "05_depth_assignments.png", rotation=0)

    # ---------- 6) Redundancia RAW vs minimizado (tags por obra) ----------
    tags_per_art_raw = read_sql(conn, """
        SELECT cat_no, COUNT(*) AS n_tags_raw
        FROM artwork_tag_raw
        GROUP BY cat_no;
    """)
    tags_per_art_min = read_sql(conn, """
        SELECT cat_no, COUNT(*) AS n_tags_min
        FROM artwork_tag
        GROUP BY cat_no;
    """)
    merged = tags_per_art_raw.merge(tags_per_art_min, on="cat_no", how="left").fillna({"n_tags_min": 0})
    merged["ratio_raw_min"] = merged["n_tags_raw"] / merged["n_tags_min"].replace(0, np.nan)

    save_csv(merged, tables_dir / "06_tags_per_artwork_raw_vs_min.csv")

    plot_hist(merged["n_tags_raw"].to_numpy(), bins=50,
              title="Distribución de nº de tags por obra (RAW)", xlabel="n_tags_raw", path=figs_dir / "06_hist_tags_raw.png")
    plot_hist(merged["n_tags_min"].to_numpy(), bins=50,
              title="Distribución de nº de tags por obra (minimizado)", xlabel="n_tags_min", path=figs_dir / "06_hist_tags_min.png")

    summary_redundancy = pd.DataFrame([{
        "avg_tags_raw": float(merged["n_tags_raw"].mean()),
        "avg_tags_min": float(merged["n_tags_min"].mean()),
        "median_tags_raw": float(merged["n_tags_raw"].median()),
        "median_tags_min": float(merged["n_tags_min"].median()),
        "total_links_raw": int(tags_per_art_raw["n_tags_raw"].sum()),
        "total_links_min": int(tags_per_art_min["n_tags_min"].sum()),
        "compression_ratio_raw_to_min": float(tags_per_art_raw["n_tags_raw"].sum() / max(1, tags_per_art_min["n_tags_min"].sum()))
    }])
    save_csv(summary_redundancy, tables_dir / "06_redundancy_summary.csv")

    # ---------- 7) Intersecciones: personas / fauna / ambos / ninguno ----------
    # (usamos raíces por slug si existen)
    def get_root_id(slug: str) -> int | None:
        df = read_sql(conn, "SELECT tag_id FROM icon_tag WHERE slug=? LIMIT 1;", (slug,))
        return int(df.iloc[0, 0]) if len(df) else None

    personas_id = get_root_id("personas")
    fauna_id = get_root_id("fauna")
    temas_id = get_root_id("temas")

    if personas_id is None or fauna_id is None:
        inter_df = pd.DataFrame([{"error": "No se encontraron raíces 'personas' y/o 'fauna' en icon_tag."}])
    else:
        inter = read_sql(conn, """
            WITH
            personas_desc AS (
              SELECT descendant_id AS tag_id
              FROM tag_closure
              WHERE ancestor_id = ?
            ),
            fauna_desc AS (
              SELECT descendant_id AS tag_id
              FROM tag_closure
              WHERE ancestor_id = ?
            ),
            has_personas AS (
              SELECT DISTINCT at.cat_no
              FROM artwork_tag at
              JOIN personas_desc pd ON pd.tag_id = at.tag_id
            ),
            has_fauna AS (
              SELECT DISTINCT at.cat_no
              FROM artwork_tag at
              JOIN fauna_desc fd ON fd.tag_id = at.tag_id
            )
            SELECT
              (SELECT COUNT(*) FROM artwork) AS total_artworks,
              (SELECT COUNT(*) FROM has_personas) AS n_personas,
              (SELECT COUNT(*) FROM has_fauna) AS n_fauna,
              (SELECT COUNT(*) FROM has_personas hp JOIN has_fauna hf ON hp.cat_no = hf.cat_no) AS n_both,
              (SELECT COUNT(*) FROM has_personas hp LEFT JOIN has_fauna hf ON hp.cat_no = hf.cat_no WHERE hf.cat_no IS NULL) AS n_personas_only,
              (SELECT COUNT(*) FROM has_fauna hf LEFT JOIN has_personas hp ON hp.cat_no = hf.cat_no WHERE hp.cat_no IS NULL) AS n_fauna_only,
              (SELECT COUNT(*) FROM artwork a
                 WHERE a.cat_no NOT IN (SELECT cat_no FROM has_personas)
                   AND a.cat_no NOT IN (SELECT cat_no FROM has_fauna)
              ) AS n_neither;
        """, (personas_id, fauna_id))
        inter_df = inter

    save_csv(inter_df, tables_dir / "07_intersections_personas_fauna.csv")

    # Bar plot para intersecciones (si no hay error)
    if "error" not in inter_df.columns:
        bars = pd.DataFrame([
            {"group": "personas", "n": int(inter_df.loc[0, "n_personas"])},
            {"group": "fauna", "n": int(inter_df.loc[0, "n_fauna"])},
            {"group": "personas∩fauna", "n": int(inter_df.loc[0, "n_both"])},
            {"group": "solo personas", "n": int(inter_df.loc[0, "n_personas_only"])},
            {"group": "solo fauna", "n": int(inter_df.loc[0, "n_fauna_only"])},
            {"group": "ninguno", "n": int(inter_df.loc[0, "n_neither"])},
        ])
        save_csv(bars, tables_dir / "07_intersections_bars.csv")
        plot_bar(bars, "group", "n", "Intersecciones: personas vs fauna", figs_dir / "07_intersections_personas_fauna.png", rotation=20)

    # ---------- 8) Religión dentro de temas (si existe) ----------
    religion_id = get_root_id("temas/religion")
    if religion_id is not None and fauna_id is not None and personas_id is not None:
        rel = read_sql(conn, """
            WITH
            rel_desc AS (SELECT descendant_id AS tag_id FROM tag_closure WHERE ancestor_id = ?),
            fauna_desc AS (SELECT descendant_id AS tag_id FROM tag_closure WHERE ancestor_id = ?),
            pers_desc AS (SELECT descendant_id AS tag_id FROM tag_closure WHERE ancestor_id = ?),
            has_rel AS (
              SELECT DISTINCT at.cat_no FROM artwork_tag at JOIN rel_desc rd ON rd.tag_id = at.tag_id
            ),
            has_fauna AS (
              SELECT DISTINCT at.cat_no FROM artwork_tag at JOIN fauna_desc fd ON fd.tag_id = at.tag_id
            ),
            has_pers AS (
              SELECT DISTINCT at.cat_no FROM artwork_tag at JOIN pers_desc pd ON pd.tag_id = at.tag_id
            )
            SELECT
              (SELECT COUNT(*) FROM has_rel) AS n_religion,
              (SELECT COUNT(*) FROM has_rel hr JOIN has_fauna hf ON hr.cat_no = hf.cat_no) AS n_religion_and_fauna,
              (SELECT COUNT(*) FROM has_rel hr JOIN has_pers hp ON hr.cat_no = hp.cat_no) AS n_religion_and_personas,
              (SELECT COUNT(*) FROM has_rel hr
                 JOIN has_fauna hf ON hr.cat_no = hf.cat_no
                 JOIN has_pers hp ON hr.cat_no = hp.cat_no) AS n_religion_and_both;
        """, (religion_id, fauna_id, personas_id))
        save_csv(rel, tables_dir / "08_religion_intersections.csv")

        rel_bars = pd.DataFrame([
            {"group": "religión", "n": int(rel.loc[0, "n_religion"])},
            {"group": "religión∩fauna", "n": int(rel.loc[0, "n_religion_and_fauna"])},
            {"group": "religión∩personas", "n": int(rel.loc[0, "n_religion_and_personas"])},
            {"group": "religión∩personas∩fauna", "n": int(rel.loc[0, "n_religion_and_both"])},
        ])
        save_csv(rel_bars, tables_dir / "08_religion_bars.csv")
        plot_bar(rel_bars, "group", "n", "Intersecciones con religión (temas/religion)", figs_dir / "08_religion_intersections.png", rotation=20)

    # ---------- 9) Alertas de calidad: posibles términos humanos dentro de fauna ----------
    # (ej. 'fauna/animales/fabulosos/hombre' detectado en tu output)
    suspect_patterns = [
        r"hombre", r"mujer", r"persona", r"hum", r"virgen", r"santo", r"angel"
    ]
    pattern = "(" + "|".join(suspect_patterns) + ")"
    suspects = read_sql(conn, """
        SELECT tag_id, name, slug, depth
        FROM icon_tag
        WHERE slug LIKE 'fauna/%'
        ORDER BY slug;
    """)
    suspects = suspects[suspects["slug"].str.contains(pattern, flags=re.IGNORECASE, regex=True)]
    save_csv(suspects, tables_dir / "09_quality_suspects_in_fauna.csv")

    # ---------- 10) Informe Markdown (resumen para slides) ----------
    report = []
    report.append("# AGRUPA — EDA de la base de datos SQLite\n")
    report.append(f"- DB: `{db_path}`")
    report.append(f"- Salida: `{out_dir}`")
    report.append(f"- Fecha: {datetime.now().isoformat(timespec='seconds')}\n")

    report.append("## Tamaños de tablas\n")
    report.append(sizes.to_markdown(index=False))
    report.append("\n\n## Raíces\n")
    report.append(roots.to_markdown(index=False))
    report.append("\n\n## Obras por raíz (expansión jerárquica)\n")
    report.append(root_counts_df.to_markdown(index=False))

    report.append("\n\n## Redundancia RAW vs minimizado\n")
    report.append(summary_redundancy.to_markdown(index=False))

    report.append("\n\n## Top tags globales (minimizado)\n")
    report.append(top_tags.to_markdown(index=False))

    if "error" in inter_df.columns:
        report.append("\n\n## Intersecciones personas/fauna\n")
        report.append(inter_df.to_markdown(index=False))
    else:
        report.append("\n\n## Intersecciones personas/fauna\n")
        report.append(inter_df.to_markdown(index=False))

    if religion_id is not None and (tables_dir / "08_religion_intersections.csv").exists():
        rel_df = pd.read_csv(tables_dir / "08_religion_intersections.csv")
        report.append("\n\n## Intersecciones con religión (temas/religion)\n")
        report.append(rel_df.to_markdown(index=False))

    report.append("\n\n## Alertas de calidad (posibles términos humanos dentro de fauna)\n")
    report.append("Archivo: `tables/09_quality_suspects_in_fauna.csv`\n")

    (out_dir / "REPORT.md").write_text("\n".join(report), encoding="utf-8")

    conn.close()

    print("\n✅ EDA completado")
    print(f"Salida: {out_dir}")
    print(f"- Tablas: {tables_dir}")
    print(f"- Figuras: {figs_dir}")
    print("Informe: REPORT.md")


if __name__ == "__main__":
    main()
