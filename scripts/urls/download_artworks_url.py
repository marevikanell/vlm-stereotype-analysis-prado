import os
import csv
import time
import random
from typing import Optional, Tuple
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://www.museodelprado.es"


def build_search_url(id_catalogo: str) -> str:
    """
    Construye la URL de búsqueda para un Nº de catálogo.
    Importante: encodeamos el valor por si incluye caracteres especiales.
    """
    return (
        f"{BASE_URL}/busqueda-obras?"
        f"cidoc:p48_has_preferred_identifier@@@cidoc:p102_has_title={quote_plus(id_catalogo)}"
    )


def wait_results_decidable(page, timeout_ms: int = 30000) -> None:
    """
    Espera a que la página esté en un estado 'decidible':
      - ya existe al menos un resultado en #panResultados, o
      - ya existe el contador de resultados #numResultadosNoRemover strong (sea 0 o >0).
    """
    page.wait_for_function(
        """() => {
            const linkCount = document.querySelectorAll(
                '#panResultados article.card-piece-gallery a.media[href]'
            ).length;

            const counter = document.querySelector('#numResultadosNoRemover strong');
            const hasCounter = !!counter && (counter.textContent || '').trim().length > 0;

            return linkCount > 0 || hasCounter;
        }""",
        timeout=timeout_ms,
    )


def get_results_count(page) -> Optional[int]:
    """
    Devuelve el número de resultados si el contador está presente; si no, None.
    """
    try:
        txt = page.locator("#numResultadosNoRemover strong").first.text_content(timeout=1000)
        if txt is None:
            return None
        txt = txt.strip()
        if not txt:
            return None
        return int("".join(ch for ch in txt if ch.isdigit()) or "0")
    except Exception:
        return None


def extract_first_result_urls(page) -> Tuple[str, str]:
    """
    Extrae:
      - URL de la obra (href del <a class="media">)
      - URL del JPG (src/data-src/srcset del <img class="img">)
    desde el primer <article.card-piece-gallery>.
    """
    url_obra = "NA"
    url_img = "NA"

    a = page.locator("#panResultados article.card-piece-gallery a.media[href]").first
    img = page.locator("#panResultados article.card-piece-gallery a.media img.img").first

    a.wait_for(state="attached", timeout=20000)

    href = a.get_attribute("href")
    if href:
        href = href.strip()
        if href.startswith("/"):
            url_obra = BASE_URL + href
        else:
            url_obra = href

    # Imagen: puede ser lazy-loaded
    src = img.get_attribute("src")
    if not src:
        src = img.get_attribute("data-src")
    if not src:
        srcset = img.get_attribute("srcset")
        if srcset:
            src = srcset.split(",")[0].strip().split(" ")[0]

    if src:
        url_img = src.strip()

    return url_obra, url_img


def fallback_image_from_artwork_page(page, url_obra: str) -> str:
    """
    Si en el listado no hay imagen, intentamos sacarla de la ficha de la obra.
    Usamos og:image / twitter:image como fallback estable.
    """
    if not url_obra or url_obra == "NA":
        return "NA"

    try:
        page.goto(url_obra, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(300)

        og = page.locator('meta[property="og:image"]').first.get_attribute("content")
        if og and og.strip():
            return og.strip()

        tw = page.locator('meta[name="twitter:image"]').first.get_attribute("content")
        if tw and tw.strip():
            return tw.strip()
    except Exception:
        pass

    return "NA"


def fetch_urls_for_id(page, id_catalogo: str, max_retries: int = 3) -> Tuple[str, str]:
    """
    Estrategia:
      1) Navega a la URL de búsqueda.
      2) Espera a estado decidible (resultados o contador).
      3) Si contador==0 -> NA (resultado 'bueno': no existe).
      4) Si hay resultados -> extrae href + img.
      5) Si falta img -> fallback en la ficha.
      6) Si no se puede decidir o extraer por fallos transitorios -> reintentos con backoff.
    """
    url = build_search_url(id_catalogo)

    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Espera fuerte a "DOM listo para decidir"
            wait_results_decidable(page, timeout_ms=30000)

            # Si el contador está y es 0, asumimos "no existe"
            count = get_results_count(page)
            if count == 0:
                return "NA", "NA"

            # Si hay resultados, extraemos
            url_obra, url_img = extract_first_result_urls(page)

            # Si no hay enlace, algo raro: reintentar
            if url_obra == "NA":
                raise RuntimeError("Página decidible pero sin enlace a obra (posible carga incompleta).")

            # Fallback de imagen si falta
            if url_img == "NA":
                url_img = fallback_image_from_artwork_page(page, url_obra)

            return url_obra, url_img

        except Exception as e:
            last_err = e

            if attempt == max_retries:
                return "NA", "NA"

            # Backoff exponencial con jitter
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0.25, 0.75)
            time.sleep(sleep_s)

            # Reset suave
            try:
                page.goto("about:blank", timeout=15000)
            except Exception:
                pass

    if last_err:
        return "NA", "NA"
    return "NA", "NA"


def procesar_listado_obras(archivo_entrada: str, archivo_salida: str) -> None:
    # 1) IDs ya procesados
    ids_procesados = set()
    if os.path.exists(archivo_salida):
        print(f"Detectado archivo de resultados previo: '{archivo_salida}'.")
        try:
            with open(archivo_salida, mode="r", encoding="utf-8") as f:
                lector_salida = csv.DictReader(f)
                for fila in lector_salida:
                    if "ID_obra" in fila and fila["ID_obra"]:
                        ids_procesados.add(fila["ID_obra"].strip())
            print(f"Se encontraron {len(ids_procesados)} obras ya procesadas en el CSV. Se saltarán.")
        except Exception as e:
            print(f"No se pudo leer el archivo de resultados previo: {e}")

    # 2) Leer IDs del archivo de entrada
    ids_catalogo_pendientes = []
    total_ids_original = 0

    print(f"\nLeyendo el archivo de entrada: {archivo_entrada}...")
    try:
        with open(archivo_entrada, mode="r", encoding="utf-8-sig") as f:
            lector = csv.DictReader(f)

            campos = lector.fieldnames
            columna_id = None

            if campos:
                for campo in campos:
                    if "Cat" in campo:
                        columna_id = campo
                        break
                if not columna_id:
                    columna_id = campos[0]

            if not columna_id:
                print("El archivo parece estar vacío o no tiene cabeceras reconocibles.")
                return

            print(f"Se ha detectado la columna: '{columna_id}'. Comprobando IDs pendientes...")

            for fila in lector:
                if fila.get(columna_id):
                    valor = fila[columna_id].strip()
                    if valor:
                        total_ids_original += 1
                        if valor not in ids_procesados:
                            ids_catalogo_pendientes.append(valor)

    except FileNotFoundError:
        print(f"Error: No se ha encontrado el archivo {archivo_entrada}.")
        return

    total_pendientes = len(ids_catalogo_pendientes)
    print(f"De {total_ids_original} IDs totales, quedan {total_pendientes} por procesar.\n")

    if total_pendientes == 0:
        print("Todas las obras ya han sido procesadas. Finalizando ejecución.")
        return

    # 3) Preparar CSV de salida
    campos_csv = ["ID_obra", "URL_obra", "URL_img"]
    modo_escritura = "a" if ids_procesados else "w"

    with open(archivo_salida, mode=modo_escritura, newline="", encoding="utf-8") as archivo_csv:
        escritor = csv.DictWriter(archivo_csv, fieldnames=campos_csv)
        if modo_escritura == "w":
            escritor.writeheader()

        print("Iniciando navegador en modo invisible...")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
            )

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
            )

            context.set_default_timeout(30000)
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for indice, id_catalogo in enumerate(ids_catalogo_pendientes, 1):
                print(f"[{indice}/{total_pendientes}] Procesando catálogo: {id_catalogo}...")

                url_obra, url_imagen = fetch_urls_for_id(page, id_catalogo, max_retries=3)

                if url_obra == "NA":
                    print("  -> NA (no se pudo extraer o no hay resultados).")
                else:
                    print("  -> Datos extraídos con éxito.")

                escritor.writerow({"ID_obra": id_catalogo, "URL_obra": url_obra, "URL_img": url_imagen})
                archivo_csv.flush()

                time.sleep(random.uniform(2, 4))

            browser.close()

    print(f"\nProceso completado. Resultados en '{archivo_salida}'.")


if __name__ == "__main__":
    archivo_csv_entrada = "IDs_unicos_N_Cat.csv"
    archivo_csv_salida = "resultados_obras_prado.csv"
    procesar_listado_obras(archivo_csv_entrada, archivo_csv_salida)
