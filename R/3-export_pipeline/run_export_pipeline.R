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
#' @importFrom progressr handlers handler_txtprogressbar with_progress progressor
#' @importFrom purrr walk
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
#' run_export_pipeline(data_example, config, overwrite = TRUE)
run_export_pipeline <- function(fao_data_raw, config, overwrite = TRUE) {
  checkmate::assert_data_frame(fao_data_raw, min.rows = 1)
  checkmate::assert_list(config, names = "named")
  checkmate::assert_flag(overwrite)

  fao_data_raw <- ensure_data_table(fao_data_raw)
  total_steps <- 2

  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    width = 40,
    clear = FALSE
  ))

  progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(total_steps))

    processed_path <- export_processed_data(
      fao_data_raw,
      config,
      base_name = "fao_data_raw",
      overwrite = overwrite
    )
    progress(
      "runing 3-export_pipeline | exporting processed data"
    )

    lists_path <- export_selected_unique_lists(fao_data_raw, config, overwrite)
    progress(
      "runing 3-export_pipeline | exporting unique lists"
    )

    list(processed_path = processed_path, lists_path = lists_path)
  })
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
