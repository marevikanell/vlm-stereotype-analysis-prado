#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(tidyr)
  library(AGRUPA)
  library(SADCAT) 
})

db_path <- "agrupa.sqlite"
if (!file.exists(db_path)) {
  stop("Error: No se encuentra agrupa.sqlite en el directorio actual.")
}

con <- dbConnect(RSQLite::SQLite(), db_path)

cat("Extrayendo todos los descriptores de la base de datos...\n")

# Extraer TODOS los descriptores
df_long <- dbGetQuery(con, "SELECT cat_no, descriptor FROM artwork_descriptor")

if (nrow(df_long) == 0) {
  dbDisconnect(con)
  stop("No se encontraron descriptores para procesar.")
}

n_obras <- length(unique(df_long$cat_no))
cat(sprintf("Transformando descriptores de %d obras a formato ancho...\n", n_obras))

# Pivotar a formato ancho
df_wide <- df_long %>%
  group_by(cat_no) %>%
  mutate(col_name = paste0("descriptor_", row_number())) %>%
  ungroup() %>%
  pivot_wider(names_from = col_name, values_from = descriptor)

cat("Calculando cobertura global...\n")
df_calc <- dict_coverage(
  df_wide,
  dict = SADCAT::Spanishdicts$Palabra,
  prefix = "descriptor_",
  out_pct = "cov_pct_global",
  out_total = "n_descriptores_fila",
  out_in_dict = "n_en_diccionario_fila"
)

cat("Calculando cobertura por todas las facetas y dimensiones (MCE y ABC)...\n")
df_calc <- dict_dim_coverage_all(
  df_calc,
  dict_df = SADCAT::Spanishdicts,
  palabra_col = "Palabra",
  prefix = "descriptor_"
)

cat("Calculando direccion por todas las facetas y dimensiones (MCE y ABC)...\n")
df_calc <- dict_dim_dirmean_all(
  df_calc,
  dict_df = SADCAT::Spanishdicts,
  palabra_col = "Palabra",
  prefix = "descriptor_"
)

cat("Limpiando dataframe final...\n")
df_final <- df_calc %>%
  select(-matches("^descriptor_[0-9]+$"))

# Guardar en nueva tabla SQLite
cat("Guardando resultados en la tabla 'artwork_sadcat'...\n")
dbWriteTable(con, "artwork_sadcat", df_final, overwrite = TRUE)

# Crear indice para cruces rapidos
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_sadcat_cat_no ON artwork_sadcat(cat_no)")

cat(sprintf("Proceso completado con exito. Se ha guardado informacion de %d variables para %d obras.\n", ncol(df_final), nrow(df_final)))

dbDisconnect(con)