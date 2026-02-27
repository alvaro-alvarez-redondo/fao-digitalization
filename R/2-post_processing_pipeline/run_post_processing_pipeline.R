# script: run post-processing pipeline
# description: source post-processing scripts and execute cleaning, data
# standardization, and unit standardization stages between import and export.

post_processing_scripts <- c(
  "20-data_audit.R",
  "21-post_processing_utilities.R",
  "22-clean_data.R",
  "23-standardize_units.R",
  "24-standardize_data.R"
)

purrr::walk(
  post_processing_scripts,
  \(script_name) {
    source(
      here::here("R", "2-post_processing_pipeline", script_name),
      echo = FALSE
    )
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
get_required_object_or_null <- function(object_name, env) {
  checkmate::assert_string(object_name, min.chars = 1)
  checkmate::assert_environment(env)

  if (!exists(object_name, envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic post-processing pipeline skipped: missing {.val {object_name}} in environment"
    )

    return(NULL)
  }

  object_value <- get(object_name, envir = env, inherits = TRUE)

  return(object_value)
}

#' @title run post-processing pipeline batch
#' @description orchestrate audit, cleaning, data standardization, and unit
#' standardization stages and attach pipeline diagnostics to final output.
#' @param raw_dt raw data.frame/data.table.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name for unit
#' standardization.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return final post-processed data.table with `pipeline_diagnostics`
#' attribute.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_post_processing_pipeline_batch <- function(
  raw_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product"
) {
  checkmate::assert_data_frame(raw_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(product_column, min.chars = 1)

  audited_dt <- audit_data_output(raw_dt, config)

  cleaned_dt <- run_cleaning_layer_batch(audited_dt, config)

  standardized_dt <- run_harmonization_layer_batch(cleaned_dt, config)

  unit_standardized_dt <- run_number_harmonization_layer_batch(
    standardized_dt,
    config,
    unit_column,
    value_column,
    product_column
  )

  pipeline_diagnostics <- list(
    audit = NULL,
    cleaning = attr(cleaned_dt, "layer_diagnostics"),
    data_standardization = attr(standardized_dt, "layer_diagnostics"),
    unit_standardization = attr(unit_standardized_dt, "layer_diagnostics")
  )

  attr(unit_standardized_dt, "pipeline_diagnostics") <- pipeline_diagnostics

  return(unit_standardized_dt)
}

# backward-compatible alias
run_clean_harmonize_pipeline_batch <- run_post_processing_pipeline_batch

#' @title run post-processing stage automatically
#' @description execute `run_post_processing_pipeline_batch()` when automatic
#' mode is enabled and required objects are available in the environment.
#' @param auto_run logical scalar controlling whether automatic execution
#' should occur.
#' @param env environment used to resolve and assign stage objects.
#' @return invisible post-processed data table when executed, otherwise
#' invisible `NULL` when skipped.
#' @importFrom checkmate assert_flag assert_environment
run_post_processing_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (
    !exists(
      "run_post_processing_pipeline_batch",
      mode = "function",
      inherits = TRUE
    )
  ) {
    cli::cli_warn(
      "automatic post-processing pipeline skipped: missing {.val run_post_processing_pipeline_batch} function"
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

  fao_data_harmonized <- run_post_processing_pipeline_batch(
    raw_dt = fao_data_raw_value,
    config = config_value,
    unit_column = "unit_name",
    value_column = "quantity",
    product_column = "item"
  )

  assign("fao_data_harmonized", fao_data_harmonized, envir = env)

  return(invisible(fao_data_harmonized))
}

# backward-compatible alias
run_clean_harmonize_pipeline_auto <- run_post_processing_pipeline_auto

run_post_processing_pipeline_auto(
  auto_run = isTRUE(getOption(
    "fao.run_post_processing_pipeline.auto",
    getOption("fao.run_clean_harmonize_pipeline.auto", TRUE)
  ))
)
