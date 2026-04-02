"""
BLIP-2 Interactive Prompt Tester (Fixed)
=========================================
Test any prompt on any image with either model variant.

Usage:
    python prompt_tester_blip2.py [--model opt|flan]

Commands:
    [Enter]         → run current prompt on current image
    p <text>        → set new prompt
    p               → clear prompt (unconditional)
    n               → next random image
    id <cat_no>     → load specific artwork
    m               → switch model (opt ↔ flan)
    batch           → run current prompt on 10 random images
    q               → quit
"""

import sys
import time
import sqlite3
import argparse
import torch
from pathlib import Path
from PIL import Image
from transformers import Blip2Processor, Blip2ForConditionalGeneration

# ── CONFIG ──────────────────────────────────────────────────────────
DB_PATH = "/home/agrupa-lab/agrupa/agrupa.sqlite"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.float16 if DEVICE == "cuda" else torch.float32

MODELS = {
    "opt":  "Salesforce/blip2-opt-2.7b",
    "flan": "Salesforce/blip2-flan-t5-xl",
}

# ── LOAD MODEL ──────────────────────────────────────────────────────
def load_model(variant):
    model_id = MODELS[variant]
    print(f"\nLoading {model_id}...")
    t0 = time.time()
    processor = Blip2Processor.from_pretrained(model_id)
    model = Blip2ForConditionalGeneration.from_pretrained(
        model_id, torch_dtype=DTYPE, device_map="auto",
    )
    print(f"Loaded in {time.time() - t0:.1f}s | VRAM: {torch.cuda.max_memory_allocated() / 1e9:.2f} GB")
    return processor, model

# ── RUN INFERENCE ───────────────────────────────────────────────────
def run_caption(processor, model, img, prompt=None):
    # Process inputs — keep on CPU first
    if prompt:
        inputs = processor(images=img, text=prompt, return_tensors="pt")
    else:
        inputs = processor(images=img, return_tensors="pt")

    # Move to GPU with correct dtype per tensor type
    inputs_gpu = {}
    for k, v in inputs.items():
        if v.is_floating_point():
            inputs_gpu[k] = v.to(DEVICE, DTYPE)
        else:
            inputs_gpu[k] = v.to(DEVICE)

    t0 = time.time()
    with torch.no_grad():
        output_ids = model.generate(**inputs_gpu, max_new_tokens=120)
    elapsed = time.time() - t0

    caption = processor.decode(output_ids[0], skip_special_tokens=True).strip()

    # For opt model: strip the prompt prefix if echoed back
    if prompt and caption.lower().startswith(prompt.lower()):
        caption = caption[len(prompt):].strip()
        if caption.startswith(":"):
            caption = caption[1:].strip()

    return caption, elapsed

# ── DB HELPERS ──────────────────────────────────────────────────────
def get_random_image(conn):
    return conn.execute(
        "SELECT cat_no, file_path FROM artwork_image ORDER BY RANDOM() LIMIT 1"
    ).fetchone()

def get_image_by_id(conn, cat_no):
    return conn.execute(
        "SELECT cat_no, file_path FROM artwork_image WHERE cat_no = ?", (cat_no,)
    ).fetchone()

# ── MAIN ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="opt", choices=["opt", "flan"])
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    current_variant = args.model
    processor, model = load_model(current_variant)

    cat_no, fpath = get_random_image(conn)
    img = Image.open(fpath).convert("RGB")
    current_prompt = None

    print("\n" + "=" * 60)
    print("BLIP-2 PROMPT TESTER")
    print("=" * 60)
    print(f"  Model:  {current_variant}")
    print(f"  Image:  {cat_no} ({img.size[0]}x{img.size[1]})")
    print(f"  Prompt: [unconditional]")
    print()
    print("Commands: p <text> | n | id <cat_no> | m | batch | q")
    print("Press Enter to run inference on current image + prompt")
    print("-" * 60)

    while True:
        try:
            cmd = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if cmd == "q":
            break

        elif cmd.startswith("p "):
            current_prompt = cmd[2:].strip()
            print(f"  Prompt: {current_prompt}")

        elif cmd == "p":
            current_prompt = None
            print("  Prompt: [unconditional]")

        elif cmd == "n":
            cat_no, fpath = get_random_image(conn)
            img = Image.open(fpath).convert("RGB")
            print(f"  Image: {cat_no} ({img.size[0]}x{img.size[1]})")

        elif cmd.startswith("id "):
            target = cmd[3:].strip()
            row = get_image_by_id(conn, target)
            if row:
                cat_no, fpath = row
                img = Image.open(fpath).convert("RGB")
                print(f"  Image: {cat_no} ({img.size[0]}x{img.size[1]})")
            else:
                print(f"  Not found: {target}")

        elif cmd == "m":
            del processor, model
            torch.cuda.empty_cache()
            current_variant = "flan" if current_variant == "opt" else "opt"
            processor, model = load_model(current_variant)
            print(f"  Now using: {current_variant}")

        elif cmd == "batch":
            rows = conn.execute(
                "SELECT cat_no, file_path FROM artwork_image ORDER BY RANDOM() LIMIT 10"
            ).fetchall()
            print(f"\n  Model: {current_variant} | Prompt: {current_prompt or '[unconditional]'}")
            print(f"  {'─' * 50}")
            for cno, fp in rows:
                try:
                    im = Image.open(fp).convert("RGB")
                    cap, t = run_caption(processor, model, im, current_prompt)
                    print(f"  [{cno}] {cap}  ({t:.2f}s)")
                except Exception as e:
                    print(f"  [{cno}] ERROR: {e}")

        elif cmd == "":
            cap, t = run_caption(processor, model, img, current_prompt)
            print(f"\n  [{cat_no}] ({img.size[0]}x{img.size[1]})")
            print(f"  Model:   {current_variant}")
            print(f"  Prompt:  {current_prompt or '[unconditional]'}")
            print(f"  Caption: {cap}")
            print(f"  Time:    {t:.2f}s")

        else:
            print("  Commands: p <text> | p | n | id <cat_no> | m | batch | q")

    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()