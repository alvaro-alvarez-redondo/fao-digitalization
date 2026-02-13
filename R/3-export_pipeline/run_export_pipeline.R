# script: run_export_pipeline.r
# description: source export components and run the export workflow for data and unique lists.

export_scripts <- c(
  "31-export_data.R",
  "32-export_lists.R"
)

purrr::walk(
  export_scripts,
  ~ source(here::here("R/3-export_pipeline", .x), echo = FALSE)
)

#' @title run export pipeline
#' @description run the export pipeline by writing the processed dataset and configured unique-value lists, then return both output paths.
#' @param fao_data_raw data frame containing records to export; validated with checkmate::assert_data_frame.
#' @param config named list containing export configuration values consumed by downstream export functions; validated with checkmate::assert_list.
#' @param overwrite logical flag indicating whether existing files should be replaced; validated with checkmate::assert_flag.
#' @return named list with two character scalars: processed_path and lists_path.
#' @importFrom checkmate assert_data_frame assert_list assert_flag
#' @importFrom purrr walk imap
#' @importFrom here here
#' @importFrom cli cli_abort
#' @examples
#' config <- list(
#'   output_dir = tempdir(),
#'   export_config = list(
#'     lists_to_export = c("country"),
#'     lists_workbook_name = "fao_unique_lists_raw"
#'   )
#' )
#' data_example <- data.frame(country = c("argentina", "brazil"))
#' progress_bar <- create_progress_bar(total = 2)
#' progress_bar$tick()
#' run_export_pipeline(data_example, config, overwrite = TRUE)
run_export_pipeline <- function(fao_data_raw, config, overwrite = TRUE) {
  checkmate::assert_data_frame(fao_data_raw, min.rows = 1)
  checkmate::assert_list(config, names = "named")
  checkmate::assert_flag(overwrite)

  fao_data_raw <- ensure_data_table(fao_data_raw)

  export_tasks <- list(
    processed = \() export_processed_data(
      fao_data_raw,
      config,
      base_name = "fao_data_raw",
      overwrite = overwrite
    ),
    lists = \() export_selected_unique_lists(
      fao_data_raw,
      config,
      overwrite
    )
  )

  progress_bar <- create_progress_bar(total = length(export_tasks), color = "cyan")

  export_paths <- purrr::imap(
    export_tasks,
    \(task_function, task_name) {
      output_path <- task_function()
      progress_bar$tick(tokens = list(task = task_name))
      output_path
    }
  )

  list(
    processed_path = export_paths$processed,
    lists_path = export_paths$lists
  )
}

if (!exists("fao_data_raw")) {
  cli::cli_abort(
    "fao_data_raw not found in the environment. make sure the import pipeline has run."
  )
}

if (!exists("config")) {
  cli::cli_abort(
    "config not found in the environment. make sure configuration has been loaded."
  )
}

export_paths <- run_export_pipeline(fao_data_raw, config, overwrite = TRUE)
