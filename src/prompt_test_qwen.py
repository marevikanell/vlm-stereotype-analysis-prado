"""
Qwen2.5-VL Interactive Prompt Tester
======================================
Test any prompt on any artwork image from the DB.

Usage:
    python prompt_tester_qwen.py

Commands:
    [Enter]         → run current prompt on current image
    p <text>        → set new prompt
    n               → next random image
    id <cat_no>     → load specific artwork
    batch           → run current prompt on 10 random images
    q               → quit
"""

import time
import sqlite3
import torch
from PIL import Image
from transformers import Qwen2_5_VLForConditionalGeneration, AutoProcessor
from qwen_vl_utils import process_vision_info

# ── CONFIG ──────────────────────────────────────────────────────────
DB_PATH = "/home/agrupa-lab/agrupa/agrupa.sqlite"
MODEL_ID = "Qwen/Qwen2.5-VL-7B-Instruct"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.float16 if DEVICE == "cuda" else torch.float32

# ── LOAD MODEL ──────────────────────────────────────────────────────
print(f"\nLoading {MODEL_ID}...")
t0 = time.time()
model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
    MODEL_ID, torch_dtype=DTYPE, device_map="auto",
)
processor = AutoProcessor.from_pretrained(MODEL_ID)
print(f"Loaded in {time.time() - t0:.1f}s | VRAM: {torch.cuda.max_memory_allocated() / 1e9:.2f} GB")

# ── RUN INFERENCE ───────────────────────────────────────────────────
def run_caption(image_path, prompt):
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": f"file://{image_path}"},
                {"type": "text", "text": prompt},
            ],
        }
    ]

    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    image_inputs, video_inputs = process_vision_info(messages)
    inputs = processor(
        text=[text],
        images=image_inputs,
        videos=video_inputs,
        padding=True,
        return_tensors="pt",
    ).to(DEVICE)

    t0 = time.time()
    with torch.no_grad():
        output_ids = model.generate(**inputs, max_new_tokens=256)

    # Strip the input tokens to get only the generated part
    generated_ids = output_ids[:, inputs.input_ids.shape[1]:]
    caption = processor.batch_decode(generated_ids, skip_special_tokens=True)[0].strip()
    elapsed = time.time() - t0

    return caption, elapsed

# ── DB HELPERS ──────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)

def get_random_image():
    return conn.execute(
        "SELECT cat_no, file_path FROM artwork_image ORDER BY RANDOM() LIMIT 1"
    ).fetchone()

def get_image_by_id(cat_no):
    return conn.execute(
        "SELECT cat_no, file_path FROM artwork_image WHERE cat_no = ?", (cat_no,)
    ).fetchone()

# ── MAIN LOOP ───────────────────────────────────────────────────────
cat_no, fpath = get_random_image()
img = Image.open(fpath).convert("RGB")
current_prompt = "Describe the people in this painting. Include their gender, appearance, clothing, posture, emotions, and social roles."

print("\n" + "=" * 60)
print("QWEN2.5-VL PROMPT TESTER")
print("=" * 60)
print(f"  Image:  {cat_no} ({img.size[0]}x{img.size[1]})")
print(f"  Prompt: {current_prompt}")
print()
print("Commands: p <text> | n | id <cat_no> | batch | q")
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
        current_prompt = "Describe the people in this painting. Include their gender, appearance, clothing, posture, emotions, and social roles."
        print(f"  Prompt reset to default")

    elif cmd == "n":
        cat_no, fpath = get_random_image()
        img = Image.open(fpath).convert("RGB")
        print(f"  Image: {cat_no} ({img.size[0]}x{img.size[1]})")

    elif cmd.startswith("id "):
        target = cmd[3:].strip()
        row = get_image_by_id(target)
        if row:
            cat_no, fpath = row
            img = Image.open(fpath).convert("RGB")
            print(f"  Image: {cat_no} ({img.size[0]}x{img.size[1]})")
        else:
            print(f"  Not found: {target}")

    elif cmd == "batch":
        rows = conn.execute(
            "SELECT cat_no, file_path FROM artwork_image ORDER BY RANDOM() LIMIT 10"
        ).fetchall()
        print(f"\n  Prompt: {current_prompt}")
        print(f"  {'─' * 50}")
        for cno, fp in rows:
            try:
                cap, t = run_caption(fp, current_prompt)
                # Truncate long captions for display
                display = cap[:200] + "..." if len(cap) > 200 else cap
                print(f"\n  [{cno}] ({t:.2f}s)")
                print(f"    {display}")
            except Exception as e:
                print(f"  [{cno}] ERROR: {e}")

    elif cmd == "":
        cap, t = run_caption(fpath, current_prompt)
        print(f"\n  [{cat_no}] ({img.size[0]}x{img.size[1]}) — {t:.2f}s")
        print(f"  Prompt: {current_prompt}")
        print(f"  Output:")
        print(f"    {cap}")

    else:
        print("  Commands: p <text> | n | id <cat_no> | batch | q")

conn.close()
print("\nDone.")