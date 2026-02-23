# script: run_export_pipeline.r
# description: source export components and run the export workflow for data and unique lists.

#' @title source export scripts
#' @description validate and source export pipeline component scripts from
#' `R/3-export_pipeline`.
#' @param script_names character vector of script file names.
#' @return invisible character vector of sourced script paths.
#' @importFrom checkmate assert_character assert_file_exists
#' @importFrom purrr map_chr walk
#' @importFrom here here
#' @examples
#' source_export_scripts(c("30-data_audit.R", "31-export_data.R"))
source_export_scripts <- function(script_names) {
  checkmate::assert_character(script_names, any.missing = FALSE, min.len = 1)

  script_paths <- purrr::map_chr(script_names, \(script_name) {
    return(here::here("R", "3-export_pipeline", script_name))
  })

  purrr::walk(script_paths, \(script_path) {
    checkmate::assert_file_exists(script_path)
    source(script_path, echo = FALSE)
    return(invisible(NULL))
  })

  return(invisible(script_paths))
}

#' @title run export pipeline
#' @description run the export pipeline by executing audit output generation,
#' writing the processed dataset, and writing configured unique-value lists,
#' then return both output paths.
#' @param fao_data_raw data frame containing records to export; validated with
#' `checkmate::assert_data_frame`.
#' @param config named list containing export configuration values consumed by downstream
#' export functions; validated with `checkmate::assert_list`.
#' @param overwrite logical flag indicating whether existing files should be replaced;
#' validated with `checkmate::assert_flag`.
#' @return named list with two character scalars: processed_path and lists_path.
#' @importFrom checkmate assert_data_frame assert_list assert_flag
#' @importFrom progressr handlers handler_txtprogressbar with_progress progressor
#' @examples
#' config <- list(
#'   output_dir = tempdir(),
#'   export_config = list(
#'     lists_to_export = c("country"),
#'     lists_workbook_name = "fao_unique_lists_raw"
#'   )
#' )
#' data_example <- data.frame(country = c("argentina", "brazil"))
#' run_export_pipeline(data_example, config, overwrite = TRUE)
run_export_pipeline <- function(fao_data_raw, config, overwrite = TRUE) {
  checkmate::assert_data_frame(fao_data_raw, min.rows = 1)
  checkmate::assert_list(config, names = "named")
  checkmate::assert_flag(overwrite)

  export_scripts <- c(
    "30-data_audit.R",
    "31-export_data.R",
    "32-export_lists.R"
  )
  source_export_scripts(export_scripts)

  fao_data_raw <- ensure_data_table(fao_data_raw)
  total_steps <- 3

  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    width = 40,
    clear = FALSE
  ))

  result <- progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(total_steps))

    fao_data_raw_inner <- audit_data_output(fao_data_raw, config)
    progress("export pipeline: running audit output")

    processed_path <- export_processed_data(
      fao_data_raw_inner,
      config,
      base_name = "fao_data_raw",
      overwrite = overwrite
    )
    progress("export pipeline: exporting processed dataset")

    lists_path <- export_selected_unique_lists(fao_data_raw_inner, config, overwrite)
    progress("export pipeline: exporting unique lists")

    return(list(processed_path = processed_path, lists_path = lists_path))
  })

  return(result)
}

#' @title run export pipeline legacy compatibility wrapper
#' @description backward-compatible wrapper that resolves missing inputs from
#' global objects and restores `export_paths` global assignment.
#' @param fao_data_raw data frame or `NULL`. when `NULL`, reads global
#' `fao_data_raw`.
#' @param config named list or `NULL`. when `NULL`, reads global `config`.
#' @param overwrite logical scalar forwarded to `run_export_pipeline()`.
#' @return named list with `processed_path` and `lists_path`.
#' @examples
#' \dontrun{
#' legacy_source_run_export_pipeline()
#' }
legacy_source_run_export_pipeline <- function(
  fao_data_raw = NULL,
  config = NULL,
  overwrite = TRUE
) {
  if (!is.null(fao_data_raw)) {
    checkmate::assert_data_frame(fao_data_raw, min.rows = 1)
  }
  if (!is.null(config)) {
    checkmate::assert_list(config, names = "named")
  }
  checkmate::assert_flag(overwrite)

  cli::cli_warn(c(
    "legacy_source_run_export_pipeline() is deprecated and will be removed in a future release",
    "i" = "migrate to explicit run_export_pipeline(fao_data_raw, config, overwrite) calls"
  ))

  resolved_data <- fao_data_raw
  resolved_config <- config

  if (is.null(resolved_data)) {
    if (!exists("fao_data_raw", envir = .GlobalEnv, inherits = FALSE)) {
      cli::cli_abort("global `fao_data_raw` was not found for legacy export wrapper")
    }

    resolved_data <- get("fao_data_raw", envir = .GlobalEnv, inherits = FALSE)
    checkmate::assert_data_frame(resolved_data, min.rows = 1)
  }

  if (is.null(resolved_config)) {
    if (!exists("config", envir = .GlobalEnv, inherits = FALSE)) {
      cli::cli_abort("global `config` was not found for legacy export wrapper")
    }

    resolved_config <- get("config", envir = .GlobalEnv, inherits = FALSE)
    checkmate::assert_list(resolved_config, names = "named")
  }

  export_paths <- run_export_pipeline(
    fao_data_raw = resolved_data,
    config = resolved_config,
    overwrite = overwrite
  )

  assign("export_paths", export_paths, envir = .GlobalEnv)

  return(export_paths)
}
