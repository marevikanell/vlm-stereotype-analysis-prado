#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import pandas as pd
from tqdm import tqdm


# -----------------------------
# Normalización (slug jerárquico)
# -----------------------------
def slugify(s: str) -> str:
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "na"


def parse_icon_parts_from_filename(xlsx_path: Path) -> List[str]:
    # Ej: Fauna_Aves_Aguila_AguilaReal.xlsx -> ["Fauna","Aves","Aguila","AguilaReal"]
    return xlsx_path.stem.split("_")


# -----------------------------
# Esquema SQL (SQLite)
# -----------------------------
DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS artwork (
  cat_no TEXT PRIMARY KEY,
  titulo TEXT,
  autor TEXT,
  escuela_obra TEXT,
  ubicacion TEXT,
  medidas TEXT,
  tecnicas TEXT,
  soporte TEXT,
  materia TEXT,
  otros_no_inv TEXT,
  fecha_ingreso TEXT,
  forma_ingreso TEXT,
  procedencia TEXT,
  tipo_objeto TEXT,
  datacion TEXT,
  tema TEXT,
  pais_ubicacion TEXT,
  comunidad_ubicacion TEXT,
  provincia_ubicacion TEXT,
  localidad_ubicacion TEXT,
  area_departamento TEXT,
  marco_obra_actual TEXT
);

CREATE TABLE IF NOT EXISTS icon_tag (
  tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  parent_id INTEGER,
  depth INTEGER DEFAULT 0,
  FOREIGN KEY(parent_id) REFERENCES icon_tag(tag_id)
);

CREATE TABLE IF NOT EXISTS source_file (
  source_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch TEXT NOT NULL,
  rel_path TEXT NOT NULL,
  imported_at TEXT DEFAULT (datetime('now')),
  UNIQUE(batch, rel_path)
);

-- Evidencia: qué archivo asignó qué tag (trazabilidad)
CREATE TABLE IF NOT EXISTS artwork_tag_raw (
  cat_no TEXT NOT NULL,
  tag_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  PRIMARY KEY (cat_no, tag_id, source_id),
  FOREIGN KEY(cat_no) REFERENCES artwork(cat_no),
  FOREIGN KEY(tag_id) REFERENCES icon_tag(tag_id),
  FOREIGN KEY(source_id) REFERENCES source_file(source_id)
);

-- Tabla “minimizada” para análisis (sin redundancias ancestro-descendiente)
CREATE TABLE IF NOT EXISTS artwork_tag (
  cat_no TEXT NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (cat_no, tag_id),
  FOREIGN KEY(cat_no) REFERENCES artwork(cat_no),
  FOREIGN KEY(tag_id) REFERENCES icon_tag(tag_id)
);

-- Cierre transitivo para queries eficientes por jerarquía
CREATE TABLE IF NOT EXISTS tag_closure (
  ancestor_id INTEGER NOT NULL,
  descendant_id INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  PRIMARY KEY (ancestor_id, descendant_id),
  FOREIGN KEY(ancestor_id) REFERENCES icon_tag(tag_id),
  FOREIGN KEY(descendant_id) REFERENCES icon_tag(tag_id)
);

CREATE INDEX IF NOT EXISTS idx_artwork_tag_tag ON artwork_tag(tag_id);
CREATE INDEX IF NOT EXISTS idx_artwork_tag_raw_tag ON artwork_tag_raw(tag_id);
CREATE INDEX IF NOT EXISTS idx_closure_ancestor ON tag_closure(ancestor_id);
CREATE INDEX IF NOT EXISTS idx_closure_desc ON tag_closure(descendant_id);
"""


COL_MAP = {
    "N. Cat.": "cat_no",
    "Título": "titulo",
    "Autor": "autor",
    "Escuela Obra": "escuela_obra",
    "Ubicación": "ubicacion",
    "Medidas": "medidas",
    "Técnicas": "tecnicas",
    "Soporte": "soporte",
    "Materia": "materia",
    "Otros Nº Inv.": "otros_no_inv",
    "Fecha Ingreso": "fecha_ingreso",
    "Forma Ingreso": "forma_ingreso",
    "Procedencia": "procedencia",
    "Tipo Objeto": "tipo_objeto",
    "Datación": "datacion",
    "Tema": "tema",
    "País Ubicación": "pais_ubicacion",
    "Comunidad Ubicación": "comunidad_ubicacion",
    "Provincia Ubicación": "provincia_ubicacion",
    "Localidad Ubicación": "localidad_ubicacion",
    "Área / Departamento": "area_departamento",
    "Marco/Obra Actual": "marco_obra_actual",
}


ARTWORK_COLS = list(COL_MAP.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", type=str, required=True, help="Ruta a la carpeta data_raw con XLSX")
    ap.add_argument("--db-path", type=str, required=True, help="Ruta al fichero SQLite a crear")
    ap.add_argument("--batch", type=str, default="data_raw", help="Etiqueta del lote (batch) para source_file")
    args = ap.parse_args()

    data_root = Path(args.data_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    batch = args.batch

    xlsx_files = sorted([p for p in data_root.rglob("*.xlsx") if p.is_file()])
    if not xlsx_files:
        raise SystemExit(f"No se encontraron .xlsx en: {data_root}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(DDL)

    cur = conn.cursor()

    # Caches en memoria para eficiencia
    tag_id_by_slug: Dict[str, int] = {}
    tag_parent: Dict[int, Optional[int]] = {}  # tag_id -> parent_id
    source_id_by_relpath: Dict[str, int] = {}

    def get_or_create_source_id(rel_path: str) -> int:
        if rel_path in source_id_by_relpath:
            return source_id_by_relpath[rel_path]
        cur.execute(
            "INSERT OR IGNORE INTO source_file(batch, rel_path) VALUES (?, ?)",
            (batch, rel_path),
        )
        cur.execute("SELECT source_id FROM source_file WHERE batch=? AND rel_path=?", (batch, rel_path))
        sid = int(cur.fetchone()[0])
        source_id_by_relpath[rel_path] = sid
        return sid

    def get_or_create_tag(parts: List[str]) -> int:
        parent_id = None
        slug_parts: List[str] = []
        last_tag_id = None

        for depth, name in enumerate(parts):
            sp = slugify(name)
            slug_parts.append(sp)
            full_slug = "/".join(slug_parts)

            if full_slug in tag_id_by_slug:
                tag_id = tag_id_by_slug[full_slug]
            else:
                cur.execute(
                    "INSERT INTO icon_tag(name, slug, parent_id, depth) VALUES (?, ?, ?, ?)",
                    (name, full_slug, parent_id, depth),
                )
                tag_id = int(cur.lastrowid)
                tag_id_by_slug[full_slug] = tag_id
                tag_parent[tag_id] = parent_id

            parent_id = tag_id
            last_tag_id = tag_id

        assert last_tag_id is not None
        return last_tag_id

    # Inserciones por lotes
    insert_artwork_sql = f"""
    INSERT OR IGNORE INTO artwork({",".join(ARTWORK_COLS)})
    VALUES ({",".join(["?"] * len(ARTWORK_COLS))})
    """

    insert_raw_sql = """
    INSERT OR IGNORE INTO artwork_tag_raw(cat_no, tag_id, source_id)
    VALUES (?, ?, ?)
    """

    # -----------------------------
    # 1) Importación RAW
    # -----------------------------
    with conn:
        for xlsx_path in tqdm(xlsx_files, desc="Importando XLSX"):
            rel_path = str(xlsx_path.relative_to(data_root))
            source_id = get_or_create_source_id(rel_path)

            parts = parse_icon_parts_from_filename(xlsx_path)
            tag_id = get_or_create_tag(parts)

            df = pd.read_excel(xlsx_path)
            df = df[df["N. Cat."].notna()].copy()  # elimina fila vacía inicial
            df = df.rename(columns=COL_MAP)

            # Normaliza cat_no a texto
            df["cat_no"] = df["cat_no"].astype(str).str.strip()

            # Inserta obras (deduplicadas por cat_no)
            rows_artwork = []
            for _, r in df.iterrows():
                row = [None if (pd.isna(r.get(c))) else str(r.get(c)) for c in ARTWORK_COLS]
                rows_artwork.append(tuple(row))
            cur.executemany(insert_artwork_sql, rows_artwork)

            # Inserta vínculos raw (obra -> tag derivado del fichero)
            rows_raw = [(cn, tag_id, source_id) for cn in df["cat_no"].tolist()]
            cur.executemany(insert_raw_sql, rows_raw)

    # -----------------------------
    # 2) Construir closure (ancestros/descendientes)
    # -----------------------------
    # Primero cargamos tag_parent de la DB por si algo no entró en cache (robustez)
    cur.execute("SELECT tag_id, parent_id FROM icon_tag")
    for tid, pid in cur.fetchall():
        tag_parent[int(tid)] = (None if pid is None else int(pid))

    closure_rows: List[Tuple[int, int, int]] = []
    for tid in tag_parent.keys():
        closure_rows.append((tid, tid, 0))
        d = 1
        p = tag_parent[tid]
        while p is not None:
            closure_rows.append((p, tid, d))
            d += 1
            p = tag_parent.get(p)

    with conn:
        cur.execute("DELETE FROM tag_closure")
        cur.executemany(
            "INSERT OR IGNORE INTO tag_closure(ancestor_id, descendant_id, depth) VALUES (?, ?, ?)",
            closure_rows,
        )

    # -----------------------------
    # 3) Minimización: quitar ancestros redundantes por obra
    # -----------------------------
    # Construimos mapa de ancestros por tag (a partir de closure)
    cur.execute("SELECT ancestor_id, descendant_id FROM tag_closure WHERE depth > 0")
    ancestors_of: Dict[int, set] = {}
    for anc, desc in cur.fetchall():
        ancestors_of.setdefault(int(desc), set()).add(int(anc))

    cur.execute("SELECT cat_no, tag_id FROM artwork_tag_raw")
    tags_by_artwork: Dict[str, set] = {}
    for cn, tid in cur.fetchall():
        tags_by_artwork.setdefault(str(cn), set()).add(int(tid))

    minimized_rows: List[Tuple[str, int]] = []
    for cn, tags in tags_by_artwork.items():
        drop = set()
        for t in tags:
            drop |= ancestors_of.get(t, set())  # todos los ancestros de t son redundantes si t está presente
        min_tags = tags - drop
        for t in min_tags:
            minimized_rows.append((cn, t))

    with conn:
        cur.execute("DELETE FROM artwork_tag")
        cur.executemany(
            "INSERT OR IGNORE INTO artwork_tag(cat_no, tag_id) VALUES (?, ?)",
            minimized_rows,
        )

    # Reporte mínimo
    cur.execute("SELECT COUNT(*) FROM artwork")
    n_art = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM icon_tag")
    n_tag = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM artwork_tag_raw")
    n_raw = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM artwork_tag")
    n_min = cur.fetchone()[0]

    print("\n✅ Importación completada")
    print(f"DB: {db_path}")
    print(f"Obras únicas (artwork): {n_art}")
    print(f"Tags (icon_tag): {n_tag}")
    print(f"Links RAW (artwork_tag_raw): {n_raw}")
    print(f"Links minimizados (artwork_tag): {n_min}")

    conn.close()


if __name__ == "__main__":
    main()
