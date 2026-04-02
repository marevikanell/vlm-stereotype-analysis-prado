"""
BLIP-2 Smoke Test
===================================
Loads 5 sample images from the DB,
runs unconditional captioning with blip2-opt-2.7b in float16,
prints captions, and logs VRAM + timing.

Usage:
    python smoke_test_blip2.py

Requires: transformers, torch, accelerate, Pillow
"""

import time
import sqlite3
import torch
from pathlib import Path
from PIL import Image
from transformers import Blip2Processor, Blip2ForConditionalGeneration

# ── CONFIG ──────────────────────────────────────────────────────────
DB_PATH = "/home/agrupa-lab/agrupa/agrupa.sqlite"  # adjust if different
MODEL_ID = "Salesforce/blip2-opt-2.7b"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.float16 if DEVICE == "cuda" else torch.float32
N_SAMPLES = 5

# ── 1. PULL 5 SAMPLE IMAGES FROM DB ────────────────────────────────
print("=" * 60)
print("BLIP-2 SMOKE TEST")
print("=" * 60)

conn = sqlite3.connect(DB_PATH)

# Get 5 images: try to get a mix via random sampling
# (religious/secular balance will be done properly in pilot 2.2)
query = """
    SELECT ai.cat_no, ai.file_path
    FROM artwork_image ai
    ORDER BY RANDOM()
    LIMIT ?
"""
samples = conn.execute(query, (N_SAMPLES,)).fetchall()
conn.close()

print(f"\nSelected {len(samples)} sample images:")
for cat_no, fpath in samples:
    exists = "✓" if Path(fpath).exists() else "✗ MISSING"
    print(f"  {cat_no} → {fpath}  [{exists}]")

# Filter to only existing files
samples = [(c, f) for c, f in samples if Path(f).exists()]
if not samples:
    print("\n❌ No valid images found. Check file_path values in artwork_image.")
    exit(1)

# ── 2. LOAD MODEL ──────────────────────────────────────────────────
print(f"\nLoading {MODEL_ID} on {DEVICE} ({DTYPE})...")
t0 = time.time()

processor = Blip2Processor.from_pretrained(MODEL_ID)
model = Blip2ForConditionalGeneration.from_pretrained(
    MODEL_ID,
    torch_dtype=DTYPE,
    device_map="auto",  # accelerate handles placement
)

load_time = time.time() - t0
print(f"Model loaded in {load_time:.1f}s")

if DEVICE == "cuda":
    vram_after_load = torch.cuda.max_memory_allocated() / 1e9
    print(f"VRAM after model load: {vram_after_load:.2f} GB")

# ── 3. RUN INFERENCE ────────────────────────────────────────────────
print("\n" + "-" * 60)
print("UNCONDITIONAL CAPTIONING")
print("-" * 60)

times = []
for cat_no, fpath in samples:
    img = Image.open(fpath).convert("RGB")
    w, h = img.size

    inputs = processor(images=img, return_tensors="pt").to(DEVICE, DTYPE)

    torch.cuda.reset_peak_memory_stats() if DEVICE == "cuda" else None
    t0 = time.time()

    with torch.no_grad():
        output_ids = model.generate(**inputs, max_new_tokens=80)

    elapsed = time.time() - t0
    times.append(elapsed)

    caption = processor.decode(output_ids[0], skip_special_tokens=True).strip()

    print(f"\n[{cat_no}] ({w}x{h})")
    print(f"  Caption: {caption}")
    print(f"  Time:    {elapsed:.2f}s")

# ── 4. SUMMARY ──────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Model:         {MODEL_ID}")
print(f"  Device:        {DEVICE} ({DTYPE})")
print(f"  Images tested: {len(samples)}")
print(f"  Avg time/img:  {sum(times)/len(times):.2f}s")
print(f"  Min time:      {min(times):.2f}s")
print(f"  Max time:      {max(times):.2f}s")

if DEVICE == "cuda":
    peak_vram = torch.cuda.max_memory_allocated() / 1e9
    print(f"  Peak VRAM:     {peak_vram:.2f} GB")

print(f"\n  Projected time for 15,455 images (single prompt):")
avg = sum(times) / len(times)
projected_h = (15455 * avg) / 3600
print(f"    {avg:.2f}s/img × 15,455 = {projected_h:.1f}h")
print(f"    × 3 prompts = {projected_h * 3:.1f}h total")

print("\n✅ Smoke test complete. If captions look reasonable, proceed to 2.1.3.")