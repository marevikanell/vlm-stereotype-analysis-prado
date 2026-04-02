#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path

def main():
    # 1. Calcular las rutas dinamicamente desde la ubicacion del script
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    
    db_path = project_root / "agrupa.sqlite"
    obras_dir = project_root / "data_raw" / "obras"

    # Validaciones iniciales
    if not db_path.exists():
        raise SystemExit(f"Error: No se encuentra la base de datos en {db_path}")
    
    if not obras_dir.exists() or not obras_dir.is_dir():
        raise SystemExit(f"Error: No se encuentra la carpeta de obras en {obras_dir}")

    print(f"Conectando a BBDD: {db_path}")
    print(f"Buscando imagenes en: {obras_dir}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 2. Asegurar que la tabla artwork tiene la columna image_path con NA por defecto
    try:
        cur.execute("ALTER TABLE artwork ADD COLUMN image_path TEXT DEFAULT 'NA';")
        print("Columna 'image_path' anadida a la tabla artwork con valor por defecto 'NA'.")
    except sqlite3.OperationalError:
        # Si la columna ya existe, actualizamos los NULL a 'NA' por si acaso
        cur.execute("UPDATE artwork SET image_path = 'NA' WHERE image_path IS NULL;")
        print("La columna 'image_path' ya existia. Se han actualizado los valores nulos a 'NA'.")

    # 3. Recopilar actualizaciones para las obras que si tienen imagen
    img_updates = []
    
    for img_path in obras_dir.glob("*.jpg"):
        cat_no = img_path.stem  # Extrae el ID
        rel_path = f"obras/{img_path.name}" 
        img_updates.append((rel_path, cat_no))

    # 4. Ejecutar actualizacion masiva para las obras encontradas
    if img_updates:
        with conn:
            cur.executemany(
                "UPDATE artwork SET image_path = ? WHERE cat_no = ?", 
                img_updates
            )

    # 5. Comprobar e imprimir resultados detallados
    cur.execute("SELECT COUNT(*) FROM artwork")
    total_obras = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM artwork WHERE image_path != 'NA'")
    n_vinculadas = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM artwork WHERE image_path = 'NA'")
    n_sin_imagen = cur.fetchone()[0]

    print("\nProceso de vinculacion completado.")
    print(f"Total de obras en la BBDD: {total_obras}")
    print(f"Obras con imagen vinculada: {n_vinculadas}")
    print(f"Obras marcadas como 'NA' (sin imagen): {n_sin_imagen}")

    conn.close()

if __name__ == "__main__":
    main()
