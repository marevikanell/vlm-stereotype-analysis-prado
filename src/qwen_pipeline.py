"""
Qwen2.5-VL Batch Inference Pipeline
=====================================
Processes all artworks with images through Qwen2.5-VL and saves
structured figure descriptions to JSON with checkpoint/resume.

Usage:
    python batch_qwen.py [--checkpoint-every 250] [--max-tokens 512]

Run inside tmux:
    tmux new -s qwen_batch
    python src/batch_qwen.py
    # Ctrl+B, then D to detach
    # tmux attach -t qwen_batch to reconnect
"""

import json
import time
import sqlite3
import argparse
import os
import gc
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

PROMPT = """For each main figure in this painting (up to 5 main figures), use this exact format:

Figure [number]: [Male/Female]
[One short paragraph describing their body posture, gaze direction, facial expression using descriptive adjectives, and whether they appear dominant or submissive, nurturing or authoritative, active or passive. Be factual and based only on visible features.]"""

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
parser.add_argument("--checkpoint-every", type=int, default=250, help="Save checkpoint every N images")
parser.add_argument("--max-tokens", type=int, default=512, help="Max tokens to generate per image")
args = parser.parse_args()

# ── CREATE OUTPUT DIR ───────────────────────────────────────────────
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, "batch_captions.json")
checkpoint_path = os.path.join(OUTPUT_DIR, "batch_checkpoint.json")
log_path = os.path.join(OUTPUT_DIR, "batch_log.txt")

# ── LOGGING ─────────────────────────────────────────────────────────
def log(msg):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ── LOAD CHECKPOINT IF EXISTS ───────────────────────────────────────
completed_ids = set()
results = []

if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "r", encoding="utf-8") as f:
        checkpoint_data = json.load(f)
    results = checkpoint_data.get("results", [])
    completed_ids = set(r["cat_no"] for r in results if r.get("caption"))
    log(f"Resuming from checkpoint: {len(completed_ids)} already completed")
else:
    log("No checkpoint found. Starting fresh.")

# ── LOAD MODEL ──────────────────────────────────────────────────────
log(f"Loading {MODEL_ID}...")
t0 = time.time()
model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
    MODEL_ID, torch_dtype=DTYPE, device_map="auto",
)
processor = AutoProcessor.from_pretrained(MODEL_ID)
load_time = time.time() - t0
log(f"Model loaded in {load_time:.1f}s | VRAM: {torch.cuda.max_memory_allocated() / 1e9:.2f} GB")

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

    with torch.no_grad():
        output_ids = model.generate(**inputs, max_new_tokens=args.max_tokens)

    generated_ids = output_ids[:, inputs.input_ids.shape[1]:]
    caption = processor.batch_decode(generated_ids, skip_special_tokens=True)[0].strip()

    # Cleanup to prevent VRAM buildup
    del inputs, output_ids, generated_ids
    torch.cuda.empty_cache()

    return caption

# ── SAVE CHECKPOINT ─────────────────────────────────────────────────
def save_checkpoint():
    checkpoint_data = {
        "results": results,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "completed": len([r for r in results if r.get("caption")]),
        "failed": len([r for r in results if not r.get("caption")]),
    }
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)

# ── BUILD IMAGE LIST ────────────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
tipo_placeholders = ",".join(["?"] * len(KEEP_TIPOS))

query = f"""
    SELECT ai.cat_no, ai.file_path, a.is_religious
    FROM artwork_image ai
    JOIN artwork a ON ai.cat_no = a.cat_no
    WHERE a.tipo_objeto IN ({tipo_placeholders})
    ORDER BY ai.cat_no
"""
all_images = conn.execute(query, KEEP_TIPOS).fetchall()
conn.close()

# Filter out already completed
pending = [(cat, path, rel) for cat, path, rel in all_images if cat not in completed_ids]

total_all = len(all_images)
total_pending = len(pending)
log(f"Total artworks with images + valid tipo: {total_all}")
log(f"Already completed: {len(completed_ids)}")
log(f"Pending: {total_pending}")

if total_pending == 0:
    log("Nothing to process. All images already completed.")
    exit(0)

# Estimate time
est_hours = (total_pending * 2.5) / 3600
log(f"Estimated time: ~{est_hours:.1f}h (at ~2.5s/image)")

# ── MAIN LOOP ───────────────────────────────────────────────────────
log(f"\nStarting batch inference...")
log(f"Checkpoint every {args.checkpoint_every} images")
log(f"Max tokens: {args.max_tokens}")
log("=" * 60)

times = []
errors = 0
batch_start = time.time()

for i, (cat_no, fpath, is_religious) in enumerate(pending):
    try:
        # Check file exists
        if not Path(fpath).exists():
            log(f"  [{i+1}/{total_pending}] {cat_no} — FILE NOT FOUND: {fpath}")
            results.append({
                "cat_no": cat_no,
                "file_path": fpath,
                "context": "religious" if is_religious else "secular",
                "caption": None,
                "error": "file_not_found",
            })
            errors += 1
            continue

        t0 = time.time()
        caption = run_caption(fpath)
        elapsed = time.time() - t0
        times.append(elapsed)

        result = {
            "cat_no": cat_no,
            "file_path": fpath,
            "context": "religious" if is_religious else "secular",
            "caption": caption,
            "inference_time_s": round(elapsed, 2),
            "caption_length": len(caption),
            "num_figures": caption.count("Figure "),
        }
        results.append(result)

        # Progress update every 50 images
        if (i + 1) % 50 == 0:
            avg_time = sum(times[-50:]) / len(times[-50:])
            remaining = (total_pending - i - 1) * avg_time / 3600
            log(f"  [{i+1}/{total_pending}] {cat_no} — {elapsed:.2f}s — "
                f"avg last 50: {avg_time:.2f}s — ~{remaining:.1f}h remaining")

    except torch.cuda.OutOfMemoryError:
        log(f"  [{i+1}/{total_pending}] {cat_no} — CUDA OOM — clearing cache and skipping")
        torch.cuda.empty_cache()
        gc.collect()
        results.append({
            "cat_no": cat_no,
            "file_path": fpath,
            "context": "religious" if is_religious else "secular",
            "caption": None,
            "error": "cuda_oom",
        })
        errors += 1

    except Exception as e:
        log(f"  [{i+1}/{total_pending}] {cat_no} — ERROR: {str(e)[:100]}")
        torch.cuda.empty_cache()
        gc.collect()
        results.append({
            "cat_no": cat_no,
            "file_path": fpath,
            "context": "religious" if is_religious else "secular",
            "caption": None,
            "error": str(e)[:200],
        })
        errors += 1

    # Checkpoint
    if (i + 1) % args.checkpoint_every == 0:
        save_checkpoint()
        log(f"  --- CHECKPOINT saved at {i+1}/{total_pending} ---")

# ── FINAL SAVE ──────────────────────────────────────────────────────
# Save final results
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

# Also save final checkpoint
save_checkpoint()

# ── SUMMARY ─────────────────────────────────────────────────────────
total_time = time.time() - batch_start
successful = len([r for r in results if r.get("caption")])
failed = len([r for r in results if not r.get("caption")])

log("\n" + "=" * 60)
log("BATCH COMPLETE")
log("=" * 60)
log(f"  Total processed this run: {total_pending}")
log(f"  Successful: {successful} (including previous checkpoint)")
log(f"  Failed: {failed}")
log(f"  Error rate: {errors/total_pending*100:.1f}%")

if times:
    log(f"  Avg time/image: {sum(times)/len(times):.2f}s")
    log(f"  Min time: {min(times):.2f}s")
    log(f"  Max time: {max(times):.2f}s")

log(f"  Total runtime: {total_time/3600:.1f}h")
log(f"\n  Output: {output_path}")
log(f"  Log: {log_path}")
log(f"\n  Next: run parse_captions.py --input {output_path}")