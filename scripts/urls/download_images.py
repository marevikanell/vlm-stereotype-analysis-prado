import os
import csv
import time
import random
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CSV_IN = "resultados_obras_prado.csv"
DEST_DIR = "obras"
FAIL_LOG = "imagenes_fallidas.csv"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Carpeta '{path}' creada.")


def load_rows(csv_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No se encontró el archivo {csv_path}")
    with open(csv_path, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def normalize_url(url: str) -> str:
    url = url.strip().replace("\\", "/")
    return url


def build_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def guess_extension(content_type: Optional[str], url: str) -> str:
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct == "image/jpeg":
            return ".jpg"
        if ct == "image/png":
            return ".png"
        if ct == "image/webp":
            return ".webp"
        if ct == "image/avif":
            return ".avif"
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".avif"):
        if url.lower().endswith(ext):
            return ".jpg" if ext == ".jpeg" else ext
    return ".jpg"


def looks_like_html(first_bytes: bytes) -> bool:
    head = first_bytes.lstrip()[:200].lower()
    return head.startswith(b"<!doctype html") or b"<html" in head or b"cloudflare" in head


def download_one(
    sess: requests.Session,
    id_obra: str,
    url_img: str,
    dest_dir: str,
    max_attempts: int = 4,
    timeout: int = 40,
) -> Tuple[bool, str, str]:
    """
    Devuelve (ok, filepath, error_msg)
    """
    url_img = normalize_url(url_img)

    ext = ".jpg"
    try:
        h = sess.head(url_img, allow_redirects=True, timeout=timeout)
        ext = guess_extension(h.headers.get("Content-Type"), url_img)
    except Exception:
        pass

    final_path = os.path.join(dest_dir, f"{id_obra}{ext}")
    tmp_path = final_path + ".part"

    if os.path.exists(final_path) and os.path.getsize(final_path) > 1024:
        return True, final_path, ""

    last_err = "NA"
    for attempt in range(1, max_attempts + 1):
        try:
            r = sess.get(url_img, stream=True, allow_redirects=True, timeout=timeout)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
                time.sleep(wait_s + random.uniform(0.2, 0.8))
                last_err = "HTTP 429 (rate limit)"
                continue

            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                time.sleep((2 ** (attempt - 1)) + random.uniform(0.2, 0.8))
                continue

            content_type = (r.headers.get("Content-Type") or "").lower()
            if content_type and "image/" not in content_type:
                first_chunk = next(r.iter_content(chunk_size=4096), b"")
                if looks_like_html(first_chunk):
                    last_err = f"Contenido no imagen (posible HTML/captcha): {content_type}"
                    time.sleep((2 ** (attempt - 1)) + random.uniform(0.2, 0.8))
                    continue
                last_err = f"Content-Type no imagen: {content_type}"
                time.sleep((2 ** (attempt - 1)) + random.uniform(0.2, 0.8))
                continue

            with open(tmp_path, "wb") as f:
                first = True
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    if first:
                        if looks_like_html(chunk):
                            raise RuntimeError("Respuesta parece HTML (bloqueo) aunque venga como 200.")
                        first = False
                    f.write(chunk)

            if os.path.getsize(tmp_path) < 1024:
                raise RuntimeError("Archivo descargado demasiado pequeño (posible error/placeholder).")

            os.replace(tmp_path, final_path)
            return True, final_path, ""

        except Exception as e:
            last_err = str(e)[:200]
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

            if attempt < max_attempts:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0.2, 0.9))

    return False, final_path, last_err


def append_fail_log(path: str, row: Dict[str, str]) -> None:
    is_new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ID_obra", "URL_img", "Motivo"])
        if is_new:
            w.writeheader()
        w.writerow(row)


def descargar_imagenes(csv_in: str, dest_dir: str) -> None:
    ensure_dir(dest_dir)
    rows = load_rows(csv_in)

    total = len(rows)
    print(f"Se han cargado {total} registros.\n")

    sess = build_session()

    descargadas = 0
    saltadas = 0
    fallidas = 0

    for i, obra in enumerate(rows, 1):
        id_obra = (obra.get("ID_obra") or "").strip()
        url_img = (obra.get("URL_img") or "").strip()

        if not id_obra:
            continue

        if not url_img or url_img == "NA":
            saltadas += 1
            continue

        print(f"[{i}/{total}] Descargando imagen {id_obra}...", end="\r")
        ok, _path_out, err = download_one(sess, id_obra, url_img, dest_dir)

        if ok:
            descargadas += 1
        else:
            fallidas += 1
            append_fail_log(FAIL_LOG, {"ID_obra": id_obra, "URL_img": url_img, "Motivo": err})
            print(f"\n  -> Fallo {id_obra}: {err}")

        time.sleep(random.uniform(0.15, 0.45))

    print("\n\nProceso finalizado.")
    print(f"Imágenes nuevas descargadas: {descargadas}")
    print(f"Registros sin URL (saltadas): {saltadas}")
    print(f"Fallidas (ver {FAIL_LOG}): {fallidas}")


if __name__ == "__main__":
    descargar_imagenes(CSV_IN, DEST_DIR)
