# script: run_export_pipeline.r
# description: source export components and run the export workflow for data and unique lists.

export_scripts <- c(
  "30-data_audit.R",
  "31-export_data.R",
  "32-export_lists.R"
)

purrr::walk(
  export_scripts,
  \(script_name) {
    source(here::here("R", "3-export_pipeline", script_name), echo = FALSE)
  }
)

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

  fao_data_raw <- ensure_data_table(fao_data_raw)
  total_steps <- 3

  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    width = 40,
    clear = FALSE
  ))

  export_result <- progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(total_steps))

    fao_data_raw <- audit_data_output(fao_data_raw, config)
    progress("export pipeline: running audit output")

    processed_path <- export_processed_data(
      fao_data_raw,
      config,
      base_name = "fao_data_raw",
      overwrite = overwrite
    )
    progress("export pipeline: exporting processed dataset")

    lists_path <- export_selected_unique_lists(fao_data_raw, config, overwrite)
    progress("export pipeline: exporting unique lists")

    list(processed_path = processed_path, lists_path = lists_path)
  })

  return(export_result)
}

#' @title run export pipeline automatically
#' @description execute `run_export_pipeline()` when automatic mode is enabled
#' and required objects are available in the environment.
#' @param auto_run logical scalar controlling automatic execution.
#' @param env environment used to resolve and assign stage objects.
#' @return invisible list with `processed_path` and `lists_path` when executed,
#' otherwise invisible `NULL` when skipped.
#' @importFrom checkmate assert_flag assert_environment
#' @importFrom cli cli_warn
#' @examples
#' run_export_pipeline_auto(auto_run = FALSE)
run_export_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (!exists("fao_data_raw", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic export pipeline skipped: missing {.val fao_data_raw} in environment"
    )
    return(invisible(NULL))
  }

  if (!exists("config", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic export pipeline skipped: missing {.val config} in environment"
    )
    return(invisible(NULL))
  }

  fao_data_raw_value <- get("fao_data_raw", envir = env, inherits = TRUE)
  config_value <- get("config", envir = env, inherits = TRUE)

  export_paths <- run_export_pipeline(
    fao_data_raw = fao_data_raw_value,
    config = config_value,
    overwrite = TRUE
  )

  assign("export_paths", export_paths, envir = env)

  return(invisible(export_paths))
}

run_export_pipeline_auto(
  auto_run = isTRUE(getOption("fao.run_export_pipeline.auto", TRUE))
)
