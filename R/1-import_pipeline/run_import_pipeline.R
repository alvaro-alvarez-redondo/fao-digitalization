# ============================================================
# Script:  run_import_pipeline.R
# Purpose: Discover, read, transform, and validate all import files
# ============================================================

# ------------------------------
# Source import pipeline scripts
# ------------------------------
import_scripts <- c(
  "10-file_io.R",
  "11-reading.R",
  "12-transform.R",
  "13-validate_log.R",
  "14-output.R"
)

purrr::walk(
  import_scripts,
  ~ source(here::here("R/1-import_pipeline", .x), echo = FALSE)
)

# ------------------------------
# 1. Discover import Excel files
# ------------------------------
file_list_dt <- discover_files(config$paths$data$imports$raw)

if (nrow(file_list_dt) == 0) {
  stop("No Excel files found. Pipeline terminated.")
}

# ------------------------------
# 2. Read all sheets
# ------------------------------
read_pipeline_result <- read_pipeline_files(file_list_dt, config)
read_data_list <- read_pipeline_result$read_data_list
collected_reading_errors <- read_pipeline_result$errors
rm(read_pipeline_result)

# ------------------------------
# 3. Transform all files (wide + long)
# ------------------------------
transformed <- transform_files_list(
  file_list_dt = file_list_dt,
  read_data_list = read_data_list,
  config = config
)

fao_data_wide_raw <- transformed$wide_raw
fao_data_long_raw <- transformed$long_raw

# ------------------------------
# 4. Validate long-format data
# ------------------------------
validation_groups <- fao_data_long_raw[, .(data = list(.SD)), by = document]
validation_results <- purrr::map(validation_groups$data, ~ validate_long_dt(.x, config))

validated_dt_list <- purrr::map(validation_results, "data")
collected_errors <- purrr::map(validation_results, "errors") |>
  unlist(use.names = FALSE)
rm(validation_groups, validation_results, read_data_list, transformed, fao_data_wide_raw)
gc()

# ------------------------------
# 5. Consolidate all validated tables
# ------------------------------
consolidated_result <- consolidate_validated_dt(validated_dt_list, config)
fao_data_raw <- consolidated_result$data
collected_warnings <- consolidated_result$warnings
rm(validated_dt_list, consolidated_result)
gc()

# ------------------------------
# End of imports setup
# ------------------------------
