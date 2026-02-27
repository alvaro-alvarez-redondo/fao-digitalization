# script: run cleaning and harmonization pipeline
# description: source cleaning/harmonization functions and execute the stage
# between import and export pipelines.

clean_harmonize_scripts <- c(
  "21-common_clean_harmonize_functions.R",
  "22-run_cleaning_stage.R",
  "24-run_general_harmonization_stage.R",
  "23-run_numeric_harmonization_stage.R"
)

purrr::walk(
  clean_harmonize_scripts,
  \(script_name) {
    source(here::here("R", "2-clean_harmonize_pipeline", script_name), echo = FALSE)
  }
)

#' @title get required object from environment or skip auto-run
#' @description validate whether a required object exists in an environment for
#' automatic pipeline execution. when the object is missing, emit a cli warning
#' and return `NULL` so callers can short-circuit cleanly.
#' @param object_name non-empty character scalar naming the required object.
#' @param env environment used for object lookup.
#' @return object value when present; otherwise `NULL`.
#' @importFrom checkmate assert_string assert_environment
#' @importFrom cli cli_warn
#' @examples
#' # internal helper used by run_clean_harmonize_pipeline_auto()
get_required_object_or_null <- function(object_name, env) {
  checkmate::assert_string(object_name, min.chars = 1)
  checkmate::assert_environment(env)

  if (!exists(object_name, envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic clean-harmonize pipeline skipped: missing {.val {object_name}} in environment"
    )

    return(NULL)
  }

  object_value <- get(object_name, envir = env, inherits = TRUE)

  return(object_value)
}

#' @title run clean-harmonize stage automatically
#' @description execute `run_clean_harmonize_pipeline()` when automatic mode is
#' enabled and required objects are available in the environment.
#' @param auto_run logical scalar controlling whether automatic execution should
#' occur.
#' @param env environment used to resolve and assign stage objects.
#' @return invisible harmonized data table when executed, otherwise invisible
#' `NULL` when skipped.
#' @importFrom checkmate assert_flag assert_environment
#' @examples
#' run_clean_harmonize_pipeline_auto(auto_run = FALSE)
run_clean_harmonize_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (!exists("run_clean_harmonize_pipeline_batch", mode = "function", inherits = TRUE)) {
    cli::cli_warn(
      "automatic clean-harmonize pipeline skipped: missing {.val run_clean_harmonize_pipeline_batch} function"
    )

    return(invisible(NULL))
  }

  fao_data_raw_value <- get_required_object_or_null("fao_data_raw", env)

  if (is.null(fao_data_raw_value)) {
    return(invisible(NULL))
  }

  config_value <- get_required_object_or_null("config", env)

  if (is.null(config_value)) {
    return(invisible(NULL))
  }

  fao_data_harmonized <- run_clean_harmonize_pipeline_batch(
    raw_dt = fao_data_raw_value,
    config = config_value,
    unit_column = "unit_name",
    value_column = "quantity",
    product_column = "item"
  )

  assign("fao_data_harmonized", fao_data_harmonized, envir = env)

  return(invisible(fao_data_harmonized))
}

run_clean_harmonize_pipeline_auto(
  auto_run = isTRUE(getOption("fao.run_clean_harmonize_pipeline.auto", TRUE))
)
