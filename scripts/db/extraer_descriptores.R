#!/usr/bin/env Rscript

# 1. Cargar librerias necesarias
suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(tidyr)
  library(udpipe)
  
  # Cargar la libreria AGRUPA
  # Si la tienes en una ruta de desarrollo, usa devtools::load_all("ruta/a/AGRUPA")
  library(AGRUPA)
})

# 2. Definir rutas (asumiendo ejecucion desde la raiz del proyecto)
db_path <- "agrupa.sqlite"
if (!file.exists(db_path)) {
  stop("Error: No se encuentra la base de datos agrupa.sqlite en el directorio actual.")
}

# 3. Conectar a la BBDD y obtener las descripciones
cat("Conectando a la BBDD...\n")
con <- dbConnect(RSQLite::SQLite(), db_path)

cat("Extrayendo obras con descripcion...\n")
obras <- dbGetQuery(con, "
  SELECT cat_no, descripcion 
  FROM artwork 
  WHERE descripcion IS NOT NULL AND descripcion != ''
")

if (nrow(obras) == 0) {
  dbDisconnect(con)
  stop("No hay descripciones validas en la base de datos para procesar.")
}

# 4. Preparar modelo UDPipe para la lematizacion
udpipe_file <- "spanish-gsd-ud-2.5-191206.udpipe"
if (!file.exists(udpipe_file)) {
  cat("Descargando modelo UDPipe para espanol...\n")
  udpipe_download_model(language = "spanish", file = udpipe_file)
}

# 5. Ejecutar la extraccion de descriptores de forma exhaustiva
cat(sprintf("Procesando descriptores de %d obras. Esto puede tardar varios minutos...\n", nrow(obras)))
df_wide <- prepare_descriptors(
  x = obras,
  input_type = "data",
  input_format = "text",
  desc_col = "descripcion",
  drop_desc_col = TRUE,
  remove_diacritics = TRUE,
  keep_enye = TRUE,
  lemmatize = "both",
  udpipe_model = udpipe_file,
  include_ngrams = TRUE,
  max_ngrams = Inf,       # Asegura la extraccion total de bigramas y trigramas
  remove_stopwords = TRUE,
  stopwords_lang = "es"
)

# Asegurar que la columna cat_no se mantiene tras la transformacion
if (!"cat_no" %in% names(df_wide)) {
  df_wide <- cbind(cat_no = obras$cat_no, df_wide)
}

# 6. Transformar de formato ancho a formato largo para la BBDD
cat("Transformando datos para estructura relacional...\n")
df_long <- df_wide %>%
  select(cat_no, starts_with("descriptor_")) %>%
  pivot_longer(
    cols = starts_with("descriptor_"),
    names_to = "tipo_columna",
    values_to = "descriptor",
    values_drop_na = TRUE
  ) %>%
  filter(descriptor != "") %>%
  select(cat_no, descriptor) %>%
  distinct() # Eliminar descriptores duplicados para la misma obra

# 7. Guardar en nueva tabla SQLite
cat("Guardando resultados en la base de datos...\n")

# Escribir la tabla. overwrite = TRUE borra la tabla si ya existia y la crea de nuevo.
dbWriteTable(con, "artwork_descriptor", df_long, overwrite = TRUE)

# Crear indices para acelerar las consultas cruzadas con la tabla principal
cat("Creando indices de busqueda...\n")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_artdesc_cat_no ON artwork_descriptor(cat_no)")
dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_artdesc_valor ON artwork_descriptor(descriptor)")

cat(sprintf("\nProceso completado. Se han extraido y guardado %d descriptores unicos en total.\n", nrow(df_long)))

dbDisconnect(con)
