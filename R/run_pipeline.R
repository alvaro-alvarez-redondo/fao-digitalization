#' run full project pipeline
#'
#' run the general, import, and export pipelines in sequence.
#'
#' @param show_view logical flag. if `TRUE`, show `fao_data_raw` in the rstudio
#'   viewer when it exists in the current environment.
#' @param pipeline_root character scalar. root folder containing the pipeline
#'   scripts.
#'
#' @return invisible `TRUE` when all pipeline scripts execute successfully.
#'
#' @examples
#' \dontrun{
#' run_pipeline(show_view = FALSE)
#' }
#'
#' @export
run_pipeline <- function(show_view = interactive(), pipeline_root = here::here("R")) {
  checkmate::assert_flag(show_view, any.missing = FALSE)
  checkmate::assert_string(pipeline_root, na.ok = FALSE, min.chars = 1)

  if (!dir.exists(pipeline_root)) {
    cli::cli_abort("pipeline root does not exist: {.path {pipeline_root}}")
  }

  pipeline_files <- c(
    file.path(pipeline_root, "0-general_pipeline", "run_general_pipeline.R"),
    file.path(pipeline_root, "1-import_pipeline", "run_import_pipeline.R"),
    file.path(pipeline_root, "3-export_pipeline", "run_export_pipeline.R")
  )

  purrr::walk(pipeline_files, function(pipeline_file) {
    if (!file.exists(pipeline_file)) {
      cli::cli_abort("required pipeline script is missing: {.path {pipeline_file}}")
    }

    cli::cli_alert_info("running pipeline script: {.path {pipeline_file}}")
    source(pipeline_file, echo = FALSE)
  })

  if (show_view && exists("fao_data_raw", inherits = TRUE)) {
    utils::View(get("fao_data_raw", inherits = TRUE))
  }

  invisible(TRUE)
}

if (isTRUE(getOption("fao.run_pipeline.auto", TRUE))) {
  run_pipeline()
}
