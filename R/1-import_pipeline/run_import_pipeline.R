# script: run import pipeline script
# description: discover, read, transform, and validate all import files

import_scripts <- c(
  "10-file_io.R",
  "11-reading.R",
  "12-transform.R",
  "13-validate_log.R",
  "14-output.R"
)

purrr::walk(
  import_scripts,
  \(script_name) {
    source(here::here("R/1-import_pipeline", script_name), echo = FALSE)
  }
)

#' @title run import pipeline
#' @description run the complete import pipeline by discovering source files,
#' reading sheets, transforming to wide and long outputs, validating each
#' document group, and consolidating validated long tables with diagnostics.
#' @param config named list containing at least `paths$data$imports$raw` as a
#' character scalar existing directory.
#' @return named list with `data` as consolidated long `data.table`, `wide_raw`
#' as transformed wide `data.table`, and `diagnostics` list with
#' `reading_errors`, `validation_errors`, and `warnings` character vectors.
#' @importFrom checkmate assert_list assert_string assert_directory_exists
#' @importFrom purrr map walk
#' @importFrom data.table copy
#' @importFrom cli cli_abort
#' @importFrom here here
#' @examples
#' # progress_bar <- create_progress_bar(total = 2)
#' # progress_bar$tick()
#' # run_import_pipeline(config)
run_import_pipeline <- function(config) {
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_string(config$paths$data$imports$raw, min.chars = 1)
  checkmate::assert_directory_exists(config$paths$data$imports$raw)

  file_list_dt <- discover_files(config$paths$data$imports$raw)

  if (nrow(file_list_dt) == 0) {
    cli::cli_abort("no excel files were found. pipeline terminated")
  }

  read_pipeline_result <- read_pipeline_files(file_list_dt, config)
  read_data_list <- read_pipeline_result$read_data_list

  transformed <- transform_files_list(
    file_list_dt = file_list_dt,
    read_data_list = read_data_list,
    config = config
  )

  validation_groups <- transformed$long_raw[,
    .(data = list(data.table::copy(.SD)[, document := .BY$document])),
    by = .(document)
  ]

  validation_results <- purrr::map(
    validation_groups$data,
    \(document_dt) validate_long_dt(document_dt, config)
  )

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

import_pipeline_result <- run_import_pipeline(config)
fao_data_raw <- import_pipeline_result$data
fao_data_wide_raw <- import_pipeline_result$wide_raw
collected_reading_errors <- import_pipeline_result$diagnostics$reading_errors
collected_errors <- import_pipeline_result$diagnostics$validation_errors
collected_warnings <- import_pipeline_result$diagnostics$warnings

rm(import_pipeline_result)
gc()
