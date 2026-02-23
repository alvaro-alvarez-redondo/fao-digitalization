#' @title run full project pipeline
#' @description run the general, import, and export pipelines in sequence and
#' populate legacy global objects used by downstream scripts.
#'
#' @param show_view logical flag. if `TRUE`, show imported data in the rstudio
#'   viewer after the import stage completes.
#' @param pipeline_root character scalar. root folder containing the pipeline
#'   scripts.
#' @return invisible `TRUE` when all pipeline scripts execute successfully.
#' side effects: assigns `config`, `import_pipeline_result`, `fao_data_raw`,
#' `fao_data_wide_raw`, `collected_reading_errors`, `collected_errors`,
#' `collected_warnings`, and `export_paths` in `.GlobalEnv`.
#' @examples
#' \dontrun{
#' run_pipeline(show_view = FALSE)
#' }
#' @export
run_pipeline <- function(
  show_view = interactive(),
  pipeline_root = here::here("R")
) {
  checkmate::assert_flag(show_view)
  checkmate::assert_string(pipeline_root, na.ok = FALSE, min.chars = 1)

  if (!dir.exists(pipeline_root)) {
    cli::cli_abort("pipeline root does not exist: {.path {pipeline_root}}")
  }

  pipeline_files <- c(
    file.path(pipeline_root, "0-general_pipeline", "run_general_pipeline.R"),
    file.path(pipeline_root, "1-import_pipeline", "run_import_pipeline.R"),
    file.path(pipeline_root, "3-export_pipeline", "run_export_pipeline.R")
  )

  purrr::walk(pipeline_files, \(pipeline_file) {
    if (!file.exists(pipeline_file)) {
      cli::cli_abort(
        "{.strong required pipeline script is missing: {.val {basename(pipeline_file)}}}"
      )
    }

    pipeline_name <- basename(pipeline_file)
    cli::cli_alert_info(
      "{.strong sourcing pipeline script: {.val {pipeline_name}}}"
    )
    source(pipeline_file, echo = FALSE)
  })

  required_functions <- c(
    "run_general_pipeline",
    "run_import_pipeline",
    "run_export_pipeline"
  )

  missing_functions <- required_functions[!vapply(
    required_functions,
    exists,
    logical(1),
    mode = "function",
    inherits = TRUE
  )]

  if (length(missing_functions) > 0) {
    cli::cli_abort(c(
      "required pipeline runner functions are unavailable after sourcing",
      "!" = "missing: {toString(missing_functions)}"
    ))
  }

  cli::cli_alert_info("{.strong executing general pipeline}")
  config <- run_general_pipeline()

  cli::cli_alert_info("{.strong executing import pipeline}")
  import_result <- run_import_pipeline(config)

  cli::cli_alert_info("{.strong executing export pipeline}")
  export_paths <- run_export_pipeline(
    fao_data_raw = import_result$data,
    config = config,
    overwrite = TRUE
  )

  assign("config", config, envir = .GlobalEnv)
  assign("import_pipeline_result", import_result, envir = .GlobalEnv)
  assign("fao_data_raw", import_result$data, envir = .GlobalEnv)
  assign("fao_data_wide_raw", import_result$wide_raw, envir = .GlobalEnv)
  assign(
    "collected_reading_errors",
    import_result$diagnostics$reading_errors,
    envir = .GlobalEnv
  )
  assign(
    "collected_errors",
    import_result$diagnostics$validation_errors,
    envir = .GlobalEnv
  )
  assign(
    "collected_warnings",
    import_result$diagnostics$warnings,
    envir = .GlobalEnv
  )
  assign("export_paths", export_paths, envir = .GlobalEnv)

  if (show_view) {
    utils::View(import_result$data)
  }

  return(invisible(TRUE))
}

#' @title run pipeline compatibility wrapper
#' @description compatibility wrapper for legacy workflows that previously
#' expected side effects during script sourcing.
#' @param show_view logical flag forwarded to `run_pipeline()`.
#' @param pipeline_root character scalar forwarded to `run_pipeline()`.
#' @return invisible `TRUE` from `run_pipeline()`.
#' @examples
#' \dontrun{
#' legacy_source_run_pipeline(show_view = FALSE)
#' }
legacy_source_run_pipeline <- function(
  show_view = interactive(),
  pipeline_root = here::here("R")
) {
  checkmate::assert_flag(show_view)
  checkmate::assert_string(pipeline_root, na.ok = FALSE, min.chars = 1)

  cli::cli_warn(c(
    "legacy_source_run_pipeline() is deprecated and will be removed in a future release",
    "i" = "migrate to explicit run_pipeline(show_view = ..., pipeline_root = ...) calls"
  ))

  return(run_pipeline(show_view = show_view, pipeline_root = pipeline_root))
}
