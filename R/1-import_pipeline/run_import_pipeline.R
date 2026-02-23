# script: run import pipeline script
# description: discover, read, transform, and validate all import files

#' @title source import scripts
#' @description validate and source import pipeline component scripts from
#' `R/1-import_pipeline`.
#' @param script_names character vector of script file names.
#' @return invisible character vector of sourced script paths.
#' @importFrom checkmate assert_character assert_file_exists
#' @importFrom purrr map_chr walk
#' @importFrom here here
#' @examples
#' source_import_scripts(c("10-file_io.R", "11-reading.R"))
source_import_scripts <- function(script_names) {
  checkmate::assert_character(script_names, any.missing = FALSE, min.len = 1)

  script_paths <- purrr::map_chr(script_names, \(script_name) {
    return(here::here("R", "1-import_pipeline", script_name))
  })

  purrr::walk(script_paths, \(script_path) {
    checkmate::assert_file_exists(script_path)
    source(script_path, echo = FALSE)
    return(invisible(NULL))
  })

  return(invisible(script_paths))
}

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
#' @importFrom checkmate assert_list assert_string assert_directory_exists
#' @importFrom purrr map
#' @importFrom cli cli_abort
#' @examples
#' # run_import_pipeline(config)
run_import_pipeline <- function(config) {
  checkmate::assert_list(config, any.missing = FALSE)
  checkmate::assert_string(config$paths$data$imports$raw, min.chars = 1)
  checkmate::assert_directory_exists(config$paths$data$imports$raw)

  build_import_folder_candidates <- function(import_folder) {
    checkmate::assert_string(import_folder, min.chars = 1)

    normalized_folder <- normalizePath(import_folder, winslash = "/", mustWork = FALSE)
    folder_name <- basename(normalized_folder)
    parent_folder <- dirname(normalized_folder)

    alternate_folder <- if (identical(folder_name, "raw imports")) {
      file.path(parent_folder, "raw")
    } else if (identical(folder_name, "raw")) {
      file.path(parent_folder, "raw imports")
    } else {
      NULL
    }

    c(normalized_folder, alternate_folder) |>
      unique() |>
      stats::na.omit() |>
      as.character()
  }

  import_scripts <- c(
    "10-file_io.R",
    "11-reading.R",
    "12-transform.R",
    "13-validate_log.R",
    "15-output.R"
  )
  source_import_scripts(import_scripts)

  import_folder_candidates <- build_import_folder_candidates(
    config$paths$data$imports$raw
  )

  discovered_file_lists <- purrr::map(import_folder_candidates, function(import_folder) {
    if (!dir.exists(import_folder)) {
      return(data.table::data.table())
    }

    discover_files(import_folder)
  })

  file_counts <- purrr::map_int(discovered_file_lists, nrow)
  selected_candidate_index <- which(file_counts > 0)[1]

  if (is.na(selected_candidate_index)) {
    file_list_dt <- data.table::data.table()
  } else {
    file_list_dt <- discovered_file_lists[[selected_candidate_index]]
  }

  if (
    length(import_folder_candidates) > 1 &&
      !is.na(selected_candidate_index) &&
      selected_candidate_index > 1
  ) {
    cli::cli_alert_info(c(
      "excel files were discovered in a legacy-compatible raw import folder",
      "i" = "configured folder: {import_folder_candidates[[1]]}",
      "i" = "selected folder: {import_folder_candidates[[selected_candidate_index]]}"
    ))
  }

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

  validation_results <- transformed$long_raw[
    ,
    .(result = list(validate_long_dt(.SD, config))),
    by = .(document)
  ]$result

  audited_dt_list <- purrr::map(validation_results, "data")

  validation_errors <- purrr::map(validation_results, "errors") |>
    unlist(use.names = FALSE)

  consolidated_result <- consolidate_audited_dt(audited_dt_list, config)

  reading_errors <- if (is.null(read_pipeline_result$errors)) {
    character(0)
  } else {
    as.character(read_pipeline_result$errors)
  }

  warnings <- if (is.null(consolidated_result$warnings)) {
    character(0)
  } else {
    as.character(consolidated_result$warnings)
  }

  result <- list(
    data = consolidated_result$data,
    wide_raw = transformed$wide_raw,
    diagnostics = list(
      reading_errors = reading_errors,
      validation_errors = as.character(validation_errors),
      warnings = warnings
    )
  )

  return(result)
}

#' @title run import pipeline legacy compatibility wrapper
#' @description backward-compatible wrapper that resolves `config` from the
#' global environment when omitted and restores legacy global assignments.
#' @param config named list or `NULL`. when `NULL`, the wrapper reads `config`
#' from `.GlobalEnv`.
#' @return named list with `data`, `wide_raw`, and `diagnostics`.
#' @examples
#' \dontrun{
#' legacy_source_run_import_pipeline()
#' }
legacy_source_run_import_pipeline <- function(config = NULL) {
  if (!is.null(config)) {
    checkmate::assert_list(config, any.missing = FALSE)
  }

  cli::cli_warn(c(
    "legacy_source_run_import_pipeline() is deprecated and will be removed in a future release",
    "i" = "migrate to explicit run_import_pipeline(config) calls"
  ))

  resolved_config <- config

  if (is.null(resolved_config)) {
    if (!exists("config", envir = .GlobalEnv, inherits = FALSE)) {
      cli::cli_abort("global `config` was not found for legacy import wrapper")
    }

    resolved_config <- get("config", envir = .GlobalEnv, inherits = FALSE)
    checkmate::assert_list(resolved_config, any.missing = FALSE)
  }

  import_pipeline_result <- run_import_pipeline(resolved_config)

  assign("import_pipeline_result", import_pipeline_result, envir = .GlobalEnv)
  assign("fao_data_raw", import_pipeline_result$data, envir = .GlobalEnv)
  assign("fao_data_wide_raw", import_pipeline_result$wide_raw, envir = .GlobalEnv)
  assign(
    "collected_reading_errors",
    import_pipeline_result$diagnostics$reading_errors,
    envir = .GlobalEnv
  )
  assign(
    "collected_errors",
    import_pipeline_result$diagnostics$validation_errors,
    envir = .GlobalEnv
  )
  assign(
    "collected_warnings",
    import_pipeline_result$diagnostics$warnings,
    envir = .GlobalEnv
  )

  return(import_pipeline_result)
}
