import os
import csv
import time
import random
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def reparar_nas(archivo_resultados):
    if not os.path.exists(archivo_resultados):
        print(f"Error: No se ha encontrado el archivo {archivo_resultados}.")
        return

    # 1. Leer todas las filas del archivo a la memoria
    print(f"Leyendo el archivo de resultados: {archivo_resultados}...")
    filas_csv = []
    with open(archivo_resultados, mode='r', encoding='utf-8') as f:
        lector = csv.DictReader(f)
        campos = lector.fieldnames
        for fila in lector:
            filas_csv.append(fila)

    # 2. Identificar cuales tienen NA
    filas_con_na = [fila for fila in filas_csv if fila['URL_obra'] == 'NA']
    total_nas = len(filas_con_na)

    print(f"Se han encontrado {total_nas} obras con 'NA' que necesitan revision.\n")

    if total_nas == 0:
        print("No hay ningun 'NA' en el archivo. Todo esta completo.")
        return

    # Funcion auxiliar para guardar el progreso de forma segura (atomic save)
    def guardar_progreso():
        archivo_temporal = archivo_resultados + '.tmp'
        with open(archivo_temporal, mode='w', newline='', encoding='utf-8') as f:
            escritor = csv.DictWriter(f, fieldnames=campos)
            escritor.writeheader()
            escritor.writerows(filas_csv)
        # Reemplazamos el archivo original por el nuevo actualizado
        os.replace(archivo_temporal, archivo_resultados)

    # 3. Iniciar el navegador con tiempos conservadores
    print("Iniciando navegador en modo conservador...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox"
            ]
        )
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True
        )
        
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        for indice, fila_na in enumerate(filas_con_na, 1):
            id_catalogo = fila_na['ID_obra']
            print(f"[{indice}/{total_nas}] Reintentando catalogo: {id_catalogo}...")
            
            url = f"https://www.museodelprado.es/busqueda-obras?cidoc:p48_has_preferred_identifier@@@cidoc:p102_has_title={id_catalogo}"
            
            url_obra_nueva = "NA"
            url_imagen_nueva = "NA"

            try:
                # Timeout general ampliado a 90 segundos
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                
                try:
                    # Timeout especifico ampliado a 30 segundos (mas conservador)
                    page.wait_for_selector('article.card-piece-gallery', timeout=30000)
                    page.wait_for_timeout(1000) 
                except Exception:
                    pass 
                
                html_content = page.content()
                soup = BeautifulSoup(html_content, 'html.parser')
                
                articulos = soup.find_all('article', class_='card-piece-gallery')
                
                if articulos:
                    for articulo in articulos:
                        enlace = articulo.find('a', class_='media')
                        if enlace:
                            href = enlace.get('href')
                            if href.startswith('/'):
                                url_obra_nueva = 'https://www.museodelprado.es' + href
                            else:
                                url_obra_nueva = href
                            
                            imagen = enlace.find('img', class_='img')
                            if imagen:
                                url_imagen_nueva = imagen.get('src')
                                
                            print("  -> Exito: Se ha recuperado la obra.")
                            break 
                
                if url_obra_nueva == "NA":
                    print("  -> Fracaso: Sigue sin aparecer. Se mantiene NA.")

            except Exception:
                print("  -> Error: Tiempo de espera agotado o bloqueo. Se mantiene NA.")

            # 4. Actualizar la fila en memoria
            fila_na['URL_obra'] = url_obra_nueva
            fila_na['URL_img'] = url_imagen_nueva
            
            # Guardar fisicamente en el disco
            guardar_progreso()
            
            # Pausa muy conservadora entre 4 y 7 segundos
            tiempo_espera = random.uniform(4, 7)
            time.sleep(tiempo_espera)

        browser.close()
        
    print(f"\nProceso de reparacion completado. Archivo '{archivo_resultados}' actualizado.")

if __name__ == "__main__":
    archivo_csv_salida = "resultados_obras_prado.csv"
    reparar_nas(archivo_csv_salida)