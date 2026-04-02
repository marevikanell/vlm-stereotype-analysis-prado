library(AGRUPA)

dic_en <- AGRUPA::load_dic("en")
figures <- read.csv("/home/agrupa-lab/agrupa/IE_capstones/Marevi/caption_outputs/pilot_figures.csv", stringsAsFactors = FALSE)

results_list <- list()

for (i in 1:nrow(figures)) {
  df_desc <- AGRUPA::prepare_descriptors(
    figures$descriptor_text[i],
    input_type = "string",
    input_format = "text",
    remove_stopwords = TRUE,
    stopwords_lang = "en",
    include_ngrams = FALSE
  )
  
  df_cov <- AGRUPA::dict_coverage(df_desc, prefix = "descriptor_", dict = dic_en$values3)
  df_dir <- AGRUPA::dict_dim_dirmean_all(df_desc, dict_df = dic_en, palabra_col = "values3")
  
  # Build base info
  base <- data.frame(
    cat_no = figures$cat_no[i],
    context = figures$context[i],
    figure_num = figures$figure_num[i],
    gender = figures$gender[i],
    descriptor_text = figures$descriptor_text[i],
    n_descriptors = df_cov$n_descriptores_fila,
    n_in_dict = df_cov$n_en_diccionario_fila,
    coverage_pct = df_cov$cov_pct_global,
    stringsAsFactors = FALSE
  )
  
  # Get all dirmean columns
  dir_cols <- grep("^dirmean_", names(df_dir), value = TRUE)
  n_cols <- grep("^n_dirmean_", names(df_dir), value = TRUE)
  
  for (col in dir_cols) {
    base[[col]] <- df_dir[[col]]
  }
  for (col in n_cols) {
    base[[col]] <- df_dir[[col]]
  }
  
  results_list[[i]] <- base
}

results <- do.call(rbind, results_list)

# Add asymmetry index
results$asymmetry <- results$dirmean_Warmth - results$dirmean_Competence

# Save
write.csv(results, "/home/agrupa-lab/agrupa/IE_capstones/Marevi/caption_outputs/pilot_sadcat_scores.csv", row.names = FALSE)

# ── SUMMARY ──
cat("=== SADCAT PILOT RESULTS ===\n")
cat("Total figures:", nrow(results), "\n")
cat("Avg coverage:", round(mean(results$coverage_pct), 1), "%\n")
cat("Figures with warmth score:", sum(!is.na(results$dirmean_Warmth)), "\n")
cat("Figures with competence score:", sum(!is.na(results$dirmean_Competence)), "\n")

cat("\n--- By Gender (all dimensions) ---\n")
dims <- c("Warmth", "Competence", "Sociability", "Morality", "Ability", "Status", "Religion", "beauty")
for (g in c("Male", "Female")) {
  sub <- results[results$gender == g, ]
  cat("\n", g, "(n =", nrow(sub), "):\n")
  for (d in dims) {
    col <- paste0("dirmean_", d)
    if (col %in% names(results)) {
      n_col <- paste0("n_dirmean_", d)
      cat("  ", d, "=", round(mean(sub[[col]], na.rm=TRUE), 3),
          "(n_scored =", sum(sub[[n_col]] > 0, na.rm=TRUE), ")\n")
    }
  }
}

cat("\n--- Asymmetry (W-C) by Context × Gender ---\n")
for (ctx in c("religious", "secular")) {
  for (g in c("Male", "Female")) {
    sub <- results[results$context == ctx & results$gender == g, ]
    if (nrow(sub) > 0) {
      cat(ctx, g, ": asymmetry =", round(mean(sub$asymmetry, na.rm=TRUE), 3),
          ", warmth =", round(mean(sub$dirmean_Warmth, na.rm=TRUE), 3),
          ", competence =", round(mean(sub$dirmean_Competence, na.rm=TRUE), 3),
          "(n =", nrow(sub), ")\n")
    }
  }
}