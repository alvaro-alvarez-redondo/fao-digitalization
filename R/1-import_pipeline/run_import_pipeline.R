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
read_data_list <- purrr::map(file_list_dt$file_path, read_file_sheets) |>
  purrr::map("data")
collected_reading_errors <- purrr::map(read_data_list, ~ .x$errors) |> unlist()

# ------------------------------
# 3. Transform all files (wide + long)
# ------------------------------
transformed <- transform_files_list(
  file_list_dt = file_list_dt,
  read_data_list = read_data_list,
  config = config,
  enable_progress = TRUE
)

fao_data_wide <- transformed$wide
fao_data_long <- transformed$long

# ------------------------------
# 4. Validate long-format data
# ------------------------------
validation_results <- split(fao_data_long, fao_data_long$document) |>
  purrr::map(~ validate_long_dt(.x, config))

validated_dt_list <- purrr::map(validation_results, "data")
collected_errors <- purrr::map(validation_results, "errors") |>
  unlist()

# ------------------------------
# 5. Consolidate all validated tables
# ------------------------------
consolidated_result <- consolidate_validated_dt(validated_dt_list, config)
fao_data_raw <- consolidated_result$data
collected_warnings <- consolidated_result$warnings

# ------------------------------
# End of imports setup
# ------------------------------
