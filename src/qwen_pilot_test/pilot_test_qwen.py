"""
Qwen2.5-VL Pilot Test
=======================
Runs the finalized prompt on a stratified sample of images
(religious + secular) and saves results to JSON for review.

Usage:
    python pilot_test_qwen.py [--n 50]
"""

import json
import time
import sqlite3
import argparse
import os
import torch
from pathlib import Path
from PIL import Image
from transformers import Qwen2_5_VLForConditionalGeneration, AutoProcessor
from qwen_vl_utils import process_vision_info

# ── CONFIG ──────────────────────────────────────────────────────────
DB_PATH = "/home/agrupa-lab/agrupa/agrupa.sqlite"
MODEL_ID = "Qwen/Qwen2.5-VL-7B-Instruct"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.float16 if DEVICE == "cuda" else torch.float32
OUTPUT_DIR = "caption_outputs"

PROMPT = """For each main figure in this painting, use this exact format:

Figure [number]: [Male/Female]
[One short paragraph describing their body posture, gaze direction, facial expression using descriptive adjectives, and whether they appear dominant or submissive, nurturing or authoritative, active or passive. Be factual and based only on visible features.]

List all figures visible in the painting."""

# Fixed filter for tipo de objetos (paintings only)
KEEP_TIPOS = (
    'Cartón para tapiz',
    'Cuadro Boceto',
    'Cuadro con marco integrado',
    'Díptico',
    'Tríptico',
    'Pequeño retrato',
    'Pintura',
    'Pintura de retablo',
    'Pintura mural',
    'Puerta de sagrario',
)

# ── PARSE ARGS ──────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--n", type=int, default=50, help="Total sample size (split evenly religious/secular)")
args = parser.parse_args()

# ── CREATE OUTPUT DIR ───────────────────────────────────────────────
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, "pilot_captions.json")

# ── LOAD MODEL ──────────────────────────────────────────────────────
print(f"\nLoading {MODEL_ID}...")
t0 = time.time()
model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
    MODEL_ID, torch_dtype=DTYPE, device_map="auto",
)
processor = AutoProcessor.from_pretrained(MODEL_ID)
print(f"Loaded in {time.time() - t0:.1f}s | VRAM: {torch.cuda.max_memory_allocated() / 1e9:.2f} GB")

# ── RUN INFERENCE ───────────────────────────────────────────────────
def run_caption(image_path):
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": f"file://{image_path}"},
                {"type": "text", "text": PROMPT},
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
        output_ids = model.generate(**inputs, max_new_tokens=512)

    generated_ids = output_ids[:, inputs.input_ids.shape[1]:]
    caption = processor.batch_decode(generated_ids, skip_special_tokens=True)[0].strip()
    elapsed = time.time() - t0

    return caption, elapsed

# ── BUILD SAMPLE ────────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
half = args.n // 2
tipo_placeholders = ",".join(["?"] * len(KEEP_TIPOS))

religious_query = f"""
    SELECT ai.cat_no, ai.file_path
    FROM artwork_image ai
    JOIN artwork a ON ai.cat_no = a.cat_no
    WHERE a.is_religious = 1
      AND a.tipo_objeto IN ({tipo_placeholders})
    ORDER BY RANDOM()
    LIMIT ?
"""
religious = conn.execute(religious_query, (*KEEP_TIPOS, half)).fetchall()

secular_query = f"""
    SELECT ai.cat_no, ai.file_path
    FROM artwork_image ai
    JOIN artwork a ON ai.cat_no = a.cat_no
    WHERE (a.is_religious = 0 OR a.is_religious IS NULL)
      AND a.tipo_objeto IN ({tipo_placeholders})
    ORDER BY RANDOM()
    LIMIT ?
"""
secular = conn.execute(secular_query, (*KEEP_TIPOS, half)).fetchall()

samples = [(cat, path, "religious") for cat, path in religious] + \
          [(cat, path, "secular") for cat, path in secular]

print(f"\nStratified sample: {len(religious)} religious + {len(secular)} secular = {len(samples)} total")
print(f"Filtered to tipo_objeto: paintings only ({len(KEEP_TIPOS)} types)")

conn.close()

# ── RUN PILOT ───────────────────────────────────────────────────────
results = []
total = len(samples)
times = []

print(f"\nRunning pilot on {total} images...")
print(f"Prompt: {PROMPT[:80]}...")
print("=" * 60)

for i, (cat_no, fpath, context) in enumerate(samples):
    try:
        caption, elapsed = run_caption(fpath)
        times.append(elapsed)

        result = {
            "cat_no": cat_no,
            "file_path": fpath,
            "context": context,
            "caption": caption,
            "inference_time_s": round(elapsed, 2),
            "caption_length": len(caption),
            "num_figures": caption.count("Figure "),
        }
        results.append(result)

        # Print progress
        fig_count = result["num_figures"]
        preview = caption[:100].replace("\n", " ") + "..."
        print(f"  [{i+1}/{total}] {cat_no} ({context}) — {elapsed:.2f}s — {fig_count} figures")
        print(f"    {preview}")

    except Exception as e:
        print(f"  [{i+1}/{total}] {cat_no} — ERROR: {e}")
        results.append({
            "cat_no": cat_no,
            "file_path": fpath,
            "context": context,
            "caption": None,
            "error": str(e),
        })

# ── SAVE RESULTS ────────────────────────────────────────────────────
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# ── PRINT SUMMARY ───────────────────────────────────────────────────
successful = [r for r in results if r.get("caption")]
failed = [r for r in results if not r.get("caption")]

print("\n" + "=" * 60)
print("PILOT SUMMARY")
print("=" * 60)
print(f"  Total:      {total}")
print(f"  Successful: {len(successful)}")
print(f"  Failed:     {len(failed)}")

if times:
    avg_time = sum(times) / len(times)
    print(f"  Avg time:   {avg_time:.2f}s/image")
    print(f"  Min time:   {min(times):.2f}s")
    print(f"  Max time:   {max(times):.2f}s")

if successful:
    avg_len = sum(r["caption_length"] for r in successful) / len(successful)
    avg_figs = sum(r["num_figures"] for r in successful) / len(successful)
    print(f"  Avg caption length: {avg_len:.0f} chars")
    print(f"  Avg figures detected: {avg_figs:.1f}")

    for ctx in ["religious", "secular"]:
        ctx_results = [r for r in successful if r["context"] == ctx]
        if ctx_results:
            ctx_avg = sum(r["caption_length"] for r in ctx_results) / len(ctx_results)
            ctx_figs = sum(r["num_figures"] for r in ctx_results) / len(ctx_results)
            print(f"  {ctx}: avg {ctx_avg:.0f} chars, avg {ctx_figs:.1f} figures")

print(f"\n  Projected full batch:")
if times:
    projected_h = (15455 * avg_time) / 3600
    print(f"    {avg_time:.2f}s/img × 15,455 = {projected_h:.1f}h")

print(f"\n  Results saved to: {output_path}")
print("  Next: parse figures and run through SADCAT.")