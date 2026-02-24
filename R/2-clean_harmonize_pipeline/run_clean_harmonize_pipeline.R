# script: run cleaning and harmonization pipeline
# description: source cleaning/harmonization functions and execute the stage
# between import and export pipelines.

source(
  here::here("R", "2-clean_harmonize_pipeline", "20-cleaning_harmonization.R"),
  echo = FALSE
)

#' @title run clean-harmonize stage automatically
#' @description execute `run_clean_harmonize_pipeline()` when automatic mode is
#' enabled and required objects are available in the environment.
#' @param auto_run logical scalar controlling whether automatic execution should
#' occur.
#' @param env environment used to resolve and assign stage objects.
#' @return invisible harmonized data table when executed, otherwise invisible
#' `NULL` when skipped.
#' @importFrom checkmate assert_flag assert_environment
#' @importFrom cli cli_warn
#' @examples
#' run_clean_harmonize_pipeline_auto(auto_run = FALSE)
run_clean_harmonize_pipeline_auto <- function(auto_run, env = parent.frame()) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (!exists("fao_data_raw", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic clean-harmonize pipeline skipped: missing {.val fao_data_raw} in environment"
    )
    return(invisible(NULL))
  }

  if (!exists("config", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic clean-harmonize pipeline skipped: missing {.val config} in environment"
    )
    return(invisible(NULL))
  }

  fao_data_raw_value <- get("fao_data_raw", envir = env, inherits = TRUE)
  config_value <- get("config", envir = env, inherits = TRUE)

  fao_data_harmonized <- run_clean_harmonize_pipeline(
    raw_dt = fao_data_raw_value,
    config = config_value,
    aggregation = TRUE
  )

  assign("fao_data_harmonized", fao_data_harmonized, envir = env)
  assign("fao_data_raw", fao_data_harmonized, envir = env)

  return(invisible(fao_data_harmonized))
}

run_clean_harmonize_pipeline_auto(
  auto_run = isTRUE(getOption("fao.run_clean_harmonize_pipeline.auto", TRUE)),
  env = parent.frame()
)
