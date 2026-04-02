#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
from pathlib import Path

def main():
    # 1. Calcular las rutas dinamicamente desde la ubicacion del script
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    
    db_path = project_root / "agrupa.sqlite"
    excel_path = project_root / "data_raw" / "descripcion_obras.xlsx"

    # Validaciones iniciales
    if not db_path.exists():
        raise SystemExit(f"Error: No se encuentra la base de datos en {db_path}")
    
    if not excel_path.exists():
        raise SystemExit(f"Error: No se encuentra el archivo Excel en {excel_path}")

    print(f"Conectando a BBDD: {db_path}")
    print(f"Leyendo Excel: {excel_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 2. Asegurar que la tabla artwork tiene la columna descripcion
    try:
        cur.execute("ALTER TABLE artwork ADD COLUMN descripcion TEXT;")
        print("Columna 'descripcion' anadida a la tabla artwork.")
    except sqlite3.OperationalError:
        # Si lanza este error, la columna ya existe
        print("La columna 'descripcion' ya existe en la base de datos.")

    # 3. Leer el archivo Excel
    # skiprows=2 salta las primeras filas hasta llegar a la cabecera real (identificador, autor, titulo, descripcion)
    try:
        df = pd.read_excel(excel_path, skiprows=2)
    except Exception as e:
        raise SystemExit(f"Error al leer el archivo Excel: {e}")

    # Limpiar nombres de columnas por si tienen espacios
    df.columns = df.columns.str.strip().str.lower()

    # Verificar que las columnas necesarias existen
    if 'identificador' not in df.columns or 'descripcion' not in df.columns:
        raise SystemExit("Error: El Excel no contiene las columnas 'identificador' o 'descripcion'.")

    # 4. Preparar los datos para la actualizacion
    # Filtramos filas donde el identificador o la descripcion sean nulos
    df_valid = df.dropna(subset=['identificador', 'descripcion']).copy()
    
    # Convertir a string y limpiar espacios
    df_valid['identificador'] = df_valid['identificador'].astype(str).str.strip()
    df_valid['descripcion'] = df_valid['descripcion'].astype(str).str.strip()

    updates = list(zip(df_valid['descripcion'], df_valid['identificador']))

    # 5. Ejecutar actualizacion masiva
    if updates:
        with conn:
            cur.executemany(
                "UPDATE artwork SET descripcion = ? WHERE cat_no = ?", 
                updates
            )

    # 6. Comprobar e imprimir resultados detallados
    cur.execute("SELECT COUNT(*) FROM artwork")
    total_obras = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM artwork WHERE descripcion IS NOT NULL AND descripcion != ''")
    n_con_descripcion = cur.fetchone()[0]

    print("\nProceso de importacion de descripciones completado.")
    print(f"Registros validos encontrados en el Excel: {len(updates)}")
    print(f"Total de obras en la BBDD: {total_obras}")
    print(f"Obras que ahora tienen descripcion en la BBDD: {n_con_descripcion}")

    conn.close()

if __name__ == "__main__":
    main()
