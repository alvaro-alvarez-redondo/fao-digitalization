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
# Function. Run full import pipeline
# ------------------------------
run_import_pipeline <- function(config) {
  file_list_dt <- discover_files(config$paths$data$imports$raw)

  if (nrow(file_list_dt) == 0) {
    stop("no excel files found. pipeline terminated.")
  }

  read_pipeline_result <- read_pipeline_files(file_list_dt, config)
  read_data_list <- read_pipeline_result$read_data_list

  transformed <- transform_files_list(
    file_list_dt = file_list_dt,
    read_data_list = read_data_list,
    config = config
  )

  validation_groups <- transformed$long_raw[, .(
    data = list(data.table::copy(.SD)[, document := .BY$document])
  ), by = .(document)]
  validation_results <- purrr::map(validation_groups$data, ~ validate_long_dt(.x, config))

  validated_dt_list <- purrr::map(validation_results, "data")
  validation_errors <- purrr::map(validation_results, "errors") |>
    unlist(use.names = FALSE)

  consolidated_result <- consolidate_validated_dt(validated_dt_list, config)

  list(
    data = consolidated_result$data,
    wide_raw = transformed$wide_raw,
    diagnostics = list(
      reading_errors = read_pipeline_result$errors,
      validation_errors = validation_errors,
      warnings = consolidated_result$warnings
    )
  )
}

# ------------------------------
# Execute automatically
# ------------------------------
import_pipeline_result <- run_import_pipeline(config)
fao_data_raw <- import_pipeline_result$data
fao_data_wide_raw <- import_pipeline_result$wide_raw
collected_reading_errors <- import_pipeline_result$diagnostics$reading_errors
collected_errors <- import_pipeline_result$diagnostics$validation_errors
collected_warnings <- import_pipeline_result$diagnostics$warnings

rm(import_pipeline_result)
gc()

# ------------------------------
# End of imports setup
# ------------------------------
