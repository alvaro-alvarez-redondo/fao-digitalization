# script: run import pipeline script
# description: discover, read, transform, and validate all import files.

import_scripts <- c(
  "10-file_io.R",
  "11-reading.R",
  "12-transform.R",
  "13-validate_log.R",
  "15-output.R"
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
#' audit execution is handled in the export pipeline stage.
#' @param config named list containing at least `paths$data$imports$raw` as a
#' character scalar directory.
#' @return named list with `data` as consolidated long `data.table`, `wide_raw`
#' as transformed wide `data.table`, and `diagnostics` list with
#' `reading_errors`, `validation_errors`, and `warnings` character vectors.
#' @importFrom checkmate assert_list assert_string assert_directory_exists assert_names assert_character assert_data_frame
#' @importFrom purrr map
#' @importFrom cli cli_abort
#' @examples
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
  checkmate::assert_names(
    names(read_pipeline_result),
    must.include = c("read_data_list", "errors")
  )
  checkmate::assert_list(
    read_pipeline_result$read_data_list,
    any.missing = TRUE
  )
  checkmate::assert_character(read_pipeline_result$errors, any.missing = FALSE)

  read_data_list <- read_pipeline_result$read_data_list

  transformed <- transform_files_list(
    file_list_dt = file_list_dt,
    read_data_list = read_data_list,
    config = config
  )

  validation_data_list <- split(
    transformed$long_raw,
    by = "document",
    keep.by = TRUE,
    sorted = FALSE
  )

  validation_results <- purrr::map(
    validation_data_list,
    \(document_dt) validate_long_dt(document_dt, config)
  )

  audited_dt_list <- purrr::map(validation_results, "data")

  validation_errors <- purrr::map(validation_results, "errors") |>
    unlist(use.names = FALSE)

  consolidated_result <- consolidate_audited_dt(audited_dt_list, config)
  checkmate::assert_names(
    names(consolidated_result),
    must.include = c("data", "warnings")
  )
  checkmate::assert_data_frame(consolidated_result$data, min.rows = 0)
  checkmate::assert_character(consolidated_result$warnings, any.missing = FALSE)

  return(list(
    data = consolidated_result$data,
    wide_raw = transformed$wide_raw,
    diagnostics = list(
      reading_errors = read_pipeline_result$errors,
      validation_errors = validation_errors,
      warnings = consolidated_result$warnings
    )
  ))
}

#' @title run import pipeline automatically
#' @description execute `run_import_pipeline()` when automatic mode is enabled
#' and a valid `config` object is available in the calling environment.
#' @param auto_run logical scalar controlling whether automatic execution should
#' occur.
#' @param env environment used to resolve and assign pipeline artifacts.
#' @return invisible named list from `run_import_pipeline()` when executed,
#' otherwise invisible `NULL`.
#' @importFrom checkmate assert_flag assert_environment
#' @importFrom cli cli_warn
#' @examples
#' run_import_pipeline_auto(auto_run = FALSE)
run_import_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (!exists("config", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic import pipeline skipped: missing {.val config} in environment"
    )
    return(invisible(NULL))
  }

  config_value <- get("config", envir = env, inherits = TRUE)
  import_pipeline_result <- run_import_pipeline(config = config_value)

  assign("fao_data_raw", import_pipeline_result$data, envir = env)
  assign("fao_data_wide_raw", import_pipeline_result$wide_raw, envir = env)
  assign(
    "collected_reading_errors",
    import_pipeline_result$diagnostics$reading_errors,
    envir = env
  )
  assign(
    "collected_errors",
    import_pipeline_result$diagnostics$validation_errors,
    envir = env
  )
  assign(
    "collected_warnings",
    import_pipeline_result$diagnostics$warnings,
    envir = env
  )

  return(invisible(import_pipeline_result))
}

run_import_pipeline_auto(
  auto_run = isTRUE(getOption("fao.run_import_pipeline.auto", TRUE))
)
