import os
import csv
import time
import random
from playwright.sync_api import sync_playwright

# =========================
# CONFIGURACIÓN DE EMERGENCIA
# =========================
CSV_IN = "resultados_obras_prado.csv"
CSV_OUT = "detalles_completos_prado.csv"

# --- CONFIGURACIÓN DE PROXY ---
# Si tienes un proxy, ponlo aquí. Si no, deja None.
# Formato: "http://usuario:password@ip:puerto" o "http://ip:puerto"
PROXY_SERVER = None 

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
]

def is_blocked(page):
    content = page.content().lower()
    # El Prado usa a veces una página blanca con un reto invisible de Cloudflare
    return any(sig in content for sig in ["cloudflare", "access denied", "captcha", "verify you are human"])

def extraer_con_evasion(page, url):
    try:
        # 1. Simular navegación humana previa
        page.goto("https://www.museodelprado.es/", wait_until="domcontentloaded")
        time.sleep(random.uniform(2, 4))
        
        # 2. Ir a la obra
        response = page.goto(url, wait_until="networkidle", timeout=60000)
        
        if response.status == 403 or is_blocked(page):
            return "BLOQUEO"

        # 3. Extraer (Selectores simplificados para mayor éxito)
        data = {
            "Nombre_obra": page.locator('h1[property*="title"]').first.inner_text().strip() if page.locator('h1[property*="title"]').count() > 0 else "NA",
            "Subtitulo": page.locator('.info-obra p').first.inner_text().strip() if page.locator('.info-obra p').count() > 0 else "NA",
            "Descripcion_detallada": " ".join(page.locator('[property="cidoc:p3_has_note"]').all_inner_texts()),
            "Etiquetas": ", ".join(page.locator('[property="muto:tagLabel"]').all_inner_texts())
        }
        return data
    except Exception as e:
        return f"ERROR: {str(e)}"

def main():
    # Cargar progreso
    processed_ids = set()
    if os.path.exists(CSV_OUT):
        with open(CSV_OUT, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            processed_ids = {row["ID_obra"] for row in reader}

    with open(CSV_IN, "r", encoding="utf-8") as f_in:
        obras = [row for row in csv.DictReader(f_in) if row["ID_obra"] not in processed_ids]

    for obra in obras:
        print(f"[*] Intentando {obra['ID_obra']}...")
        
        with sync_playwright() as p:
            launch_args = {
                "headless": True,
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process"
                ]
            }
            
            # Aplicar proxy si existe
            if PROXY_SERVER:
                launch_args["proxy"] = {"server": PROXY_SERVER}

            browser = p.chromium.launch(**launch_args)
            context = browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = context.new_page()

            resultado = extraer_con_evasion(page, obra["URL_obra"])

            if resultado == "BLOQUEO":
                print(f"  [!!!] IP bloqueada totalmente. Esperando 20 min antes de reintentar...")
                browser.close()
                time.sleep(1200) # 20 minutos de silencio total
                continue

            if isinstance(resultado, dict):
                with open(CSV_OUT, "a", encoding="utf-8", newline="") as f_out:
                    writer = csv.DictWriter(f_out, fieldnames=["ID_obra", "Nombre_obra", "Subtitulo", "Descripcion_detallada", "Etiquetas"])
                    if f_out.tell() == 0: writer.writeheader()
                    resultado["ID_obra"] = obra["ID_obra"]
                    writer.writerow(resultado)
                print(f"  [OK] Guardado.")
            
            browser.close()
            # Tiempo entre obras para no levantar sospechas
            time.sleep(random.uniform(15, 30))

if __name__ == "__main__":
    main()
