# ============================================================
# SADCAT Scoring: Batch Figures
# ============================================================
# Scores all parsed figures from Qwen2.5-VL output using
# SADCAT English dictionary via AGRUPA package.
#
# Input:  caption_outputs/batch_figures.csv
# Output: caption_outputs/batch_sadcat_scores.csv
#         caption_outputs/sadcat_scoring_report.txt
# ============================================================

library(AGRUPA)

# ── CONFIG ──────────────────────────────────────────────────
input_path  <- "/home/agrupa-lab/agrupa/IE_capstones/Marevi/caption_outputs/batch_figures.csv"
output_path <- "/home/agrupa-lab/agrupa/IE_capstones/Marevi/caption_outputs/batch_sadcat_scores.csv"
report_path <- "/home/agrupa-lab/agrupa/IE_capstones/Marevi/caption_outputs/sadcat_scoring_report.txt"

# Load English dictionary
dic_en <- AGRUPA::load_dic("en")

# ── LOAD DATA ───────────────────────────────────────────────
figures <- read.csv(input_path, stringsAsFactors = FALSE)
cat("Loaded", nrow(figures), "figures from", input_path, "\n")

# ── SCORE ALL FIGURES ───────────────────────────────────────
cat("Scoring figures (this may take a few minutes)...\n")

results_list <- vector("list", nrow(figures))
t0 <- Sys.time()

for (i in seq_len(nrow(figures))) {
  
  text <- figures$descriptor_text[i]
  
  # Prepare descriptors
  df_desc <- tryCatch(
    AGRUPA::prepare_descriptors(
      text,
      input_type = "string",
      input_format = "text",
      remove_stopwords = TRUE,
      stopwords_lang = "en",
      include_ngrams = FALSE
    ),
    error = function(e) NULL
  )
  
  if (is.null(df_desc)) {
    results_list[[i]] <- data.frame(
      cat_no       = figures$cat_no[i],
      context      = figures$context[i],
      figure_num   = figures$figure_num[i],
      gender       = figures$gender[i],
      qualifier    = figures$qualifier[i],
      n_descriptors = NA,
      n_in_dict     = NA,
      coverage_pct  = NA,
      error         = "prepare_descriptors_failed",
      stringsAsFactors = FALSE
    )
    next
  }
  
  # Coverage (English dictionary)
  df_cov <- tryCatch(
    AGRUPA::dict_coverage(df_desc, prefix = "descriptor_", dict = dic_en$values3),
    error = function(e) NULL
  )
  
  # Direction scores (all dimensions, English dictionary)
  df_dir <- tryCatch(
    AGRUPA::dict_dim_dirmean_all(df_desc, dict_df = dic_en, palabra_col = "values3"),
    error = function(e) NULL
  )
  
  # Build result row
  row <- data.frame(
    cat_no        = figures$cat_no[i],
    context       = figures$context[i],
    figure_num    = figures$figure_num[i],
    gender        = figures$gender[i],
    qualifier     = figures$qualifier[i],
    n_descriptors = if (!is.null(df_cov)) df_cov$n_descriptores_fila else NA,
    n_in_dict     = if (!is.null(df_cov)) df_cov$n_en_diccionario_fila else NA,
    coverage_pct  = if (!is.null(df_cov)) df_cov$cov_pct_global else NA,
    stringsAsFactors = FALSE
  )
  
  # Add all direction scores
  if (!is.null(df_dir)) {
    dir_cols <- grep("^dirmean_", names(df_dir), value = TRUE)
    n_cols   <- grep("^n_dirmean_", names(df_dir), value = TRUE)
    for (col in c(dir_cols, n_cols)) {
      row[[col]] <- df_dir[[col]]
    }
  }
  
  results_list[[i]] <- row
  
  # Progress every 1000
  if (i %% 1000 == 0) {
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
    rate <- i / elapsed
    remaining <- (nrow(figures) - i) / rate
    cat(sprintf("  [%d/%d] %.1f min elapsed, ~%.1f min remaining\n",
                i, nrow(figures), elapsed, remaining))
  }
}

total_time <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
cat(sprintf("Scoring complete in %.1f minutes\n", total_time))

# ── COMBINE RESULTS ─────────────────────────────────────────
results <- do.call(rbind, results_list)

# Add asymmetry index
results$asymmetry <- results$dirmean_Warmth - results$dirmean_Competence

# ── SAVE ────────────────────────────────────────────────────
write.csv(results, output_path, row.names = FALSE)
cat("Saved scores to", output_path, "\n")

# ── REPORT ──────────────────────────────────────────────────
sink(report_path)

cat("============================================================\n")
cat("SADCAT BATCH SCORING REPORT\n")
cat("============================================================\n")
cat(sprintf("  Total figures:          %d\n", nrow(results)))
cat(sprintf("  Scoring time:           %.1f minutes\n", total_time))
cat(sprintf("  Avg coverage:           %.1f%%\n", mean(results$coverage_pct, na.rm = TRUE)))
cat(sprintf("  Median coverage:        %.1f%%\n", median(results$coverage_pct, na.rm = TRUE)))
cat(sprintf("  Min coverage:           %.1f%%\n", min(results$coverage_pct, na.rm = TRUE)))
cat(sprintf("  Max coverage:           %.1f%%\n", max(results$coverage_pct, na.rm = TRUE)))
cat(sprintf("  Zero coverage:          %d\n", sum(results$coverage_pct == 0, na.rm = TRUE)))
cat(sprintf("  Figures w/ warmth:      %d\n", sum(!is.na(results$dirmean_Warmth))))
cat(sprintf("  Figures w/ competence:  %d\n", sum(!is.na(results$dirmean_Competence))))
cat(sprintf("  Scoring errors:         %d\n", sum(!is.na(results$error))))

cat("\n--- By Gender ---\n")
for (g in c("Male", "Female", "Unknown")) {
  sub <- results[results$gender == g & !is.na(results$gender), ]
  if (nrow(sub) > 0) {
    w <- mean(sub$dirmean_Warmth, na.rm = TRUE)
    c_val <- mean(sub$dirmean_Competence, na.rm = TRUE)
    a <- mean(sub$asymmetry, na.rm = TRUE)
    cat(sprintf("  %s (n=%d): warmth=%.3f, competence=%.3f, asymmetry=%.3f\n",
                g, nrow(sub), w, c_val, a))
  }
}

cat("\n--- By Context x Gender ---\n")
for (ctx in c("religious", "secular")) {
  for (g in c("Male", "Female")) {
    sub <- results[results$context == ctx & results$gender == g, ]
    if (nrow(sub) > 0) {
      w <- mean(sub$dirmean_Warmth, na.rm = TRUE)
      c_val <- mean(sub$dirmean_Competence, na.rm = TRUE)
      a <- mean(sub$asymmetry, na.rm = TRUE)
      cat(sprintf("  %s %s (n=%d): warmth=%.3f, competence=%.3f, asymmetry=%.3f\n",
                  ctx, g, nrow(sub), w, c_val, a))
    }
  }
}

cat("\n--- All Dimensions by Gender ---\n")
dims <- c("Warmth", "Competence", "Sociability", "Morality", "Ability",
          "Assertiveness", "Status", "Beliefs", "health", "beauty",
          "Religion", "deviance", "Politics")
for (g in c("Male", "Female")) {
  sub <- results[results$gender == g, ]
  cat(sprintf("\n  %s (n=%d):\n", g, nrow(sub)))
  for (d in dims) {
    col <- paste0("dirmean_", d)
    n_col <- paste0("n_dirmean_", d)
    if (col %in% names(results)) {
      val <- mean(sub[[col]], na.rm = TRUE)
      n_scored <- sum(sub[[n_col]] > 0, na.rm = TRUE)
      cat(sprintf("    %-18s = %+.3f  (scored in %d figures)\n", d, val, n_scored))
    }
  }
}

cat("\n--- Asymmetry Distribution ---\n")
valid_asym <- results$asymmetry[!is.na(results$asymmetry)]
cat(sprintf("  Mean:   %.3f\n", mean(valid_asym)))
cat(sprintf("  Median: %.3f\n", median(valid_asym)))
cat(sprintf("  SD:     %.3f\n", sd(valid_asym)))
cat(sprintf("  Min:    %.3f\n", min(valid_asym)))
cat(sprintf("  Max:    %.3f\n", max(valid_asym)))

cat("\n============================================================\n")
cat("Output:", output_path, "\n")
cat("Next: hypothesis testing (H1-H3)\n")

sink()

# Also print report to console
cat(readLines(report_path), sep = "\n")