# script: run post-processing pipeline
# description: source post-processing scripts and execute deterministic clean and
# standardize harmonization with structured audit persistence.

#' @title Source one post-processing script
#' @description Sources a single script with deterministic error handling.
#' @param script_path Character scalar script path.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_string
source_post_processing_script <- function(script_path) {
  checkmate::assert_string(script_path, min.chars = 1)

  if (!file.exists(script_path)) {
    cli::cli_abort("Required post-processing script not found: {.path {script_path}}")
  }

  tryCatch(
    {
      source(script_path, local = FALSE, echo = FALSE)
      return(invisible(TRUE))
    },
    error = function(error_condition) {
      cli::cli_abort(c(
        "Failed while sourcing post-processing script.",
        "x" = "script: {.path {script_path}}",
        "x" = "details: {error_condition$message}"
      ))
    }
  )
}

#' @title Source post-processing scripts in deterministic order
#' @description Sources required scripts for clean and standardize workflow.
#' @param pipeline_root Character scalar path to post-processing script folder.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_string
source_post_processing_scripts <- function(
  pipeline_root = here::here("scripts", "2-post_processing_pipeline")
) {
  checkmate::assert_string(pipeline_root, min.chars = 1)

  script_names <- c(
    "21-post_processing_utilities.R",
    "22-clean_data.R",
    "24-standardize_data.R",
    "25-post_processing_diagnostics.R"
  )

  purrr::walk(script_names, function(script_name) {
    source_post_processing_script(fs::path(pipeline_root, script_name))
  })

  return(invisible(TRUE))
}

source_post_processing_scripts()

#' @title Get required object from environment or return `NULL`
#' @description Retrieves an object when present; otherwise warns and returns
#' `NULL` for deterministic auto-run short-circuit behavior.
#' @param object_name Character scalar object name.
#' @param env Environment to query.
#' @return Object value or `NULL`.
#' @importFrom checkmate assert_string assert_environment
get_required_object_or_null <- function(object_name, env) {
  checkmate::assert_string(object_name, min.chars = 1)
  checkmate::assert_environment(env)

  if (!exists(object_name, envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "Automatic post-processing pipeline skipped: missing {.val {object_name}} in environment."
    )

    return(NULL)
  }

  return(get(object_name, envir = env, inherits = TRUE))
}

#' @title Persist post-processed dataset
#' @description Writes final post-processed dataset to configured processed export path.
#' @param dataset_dt Post-processed dataset.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset name.
#' @return Character scalar output file path.
#' @importFrom checkmate assert_data_frame assert_list assert_string
persist_post_processed_dataset <- function(dataset_dt, config, dataset_name) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(config$paths$data$exports$processed, min.chars = 1)

  output_dir <- config$paths$data$exports$processed
  fs::dir_create(output_dir, recurse = TRUE)

  output_path <- fs::path(output_dir, paste0(dataset_name, "_post_processed.xlsx"))

  openxlsx::write.xlsx(
    x = data.table::as.data.table(dataset_dt),
    file = output_path,
    overwrite = TRUE
  )

  return(output_path)
}

#' @title Run post-processing pipeline batch
#' @description Runs deterministic preflight, clean harmonization, standardize
#' harmonization, and persistence of dataset and audit artifacts.
#' @param raw_dt Raw dataset.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Post-processed `data.table` with `pipeline_diagnostics` attribute.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_post_processing_pipeline_batch <- function(
  raw_dt,
  config,
  dataset_name = "fao_data_raw"
) {
  checkmate::assert_data_frame(raw_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  preflight_result <- collect_post_processing_preflight(
    config = config,
    dataset_columns = colnames(raw_dt),
    expected_columns = colnames(raw_dt)
  )
  assert_post_processing_preflight(preflight_result)

  execution_timestamp_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  cleaned_dt <- run_cleaning_layer_batch(
    dataset_dt = raw_dt,
    config = config,
    dataset_name = dataset_name
  )

  standardized_dt <- run_harmonization_layer_batch(
    dataset_dt = cleaned_dt,
    config = config,
    dataset_name = dataset_name
  )

  clean_audit <- attr(cleaned_dt, "layer_audit")
  standardize_audit <- attr(standardized_dt, "layer_audit")

  audit_output_path <- persist_post_processing_audit(
    clean_audit_dt = clean_audit,
    standardize_audit_dt = standardize_audit,
    dataset_name = dataset_name,
    execution_timestamp_utc = execution_timestamp_utc
  )

  dataset_output_path <- persist_post_processed_dataset(
    dataset_dt = standardized_dt,
    config = config,
    dataset_name = dataset_name
  )

  diagnostics <- list(
    clean = attr(cleaned_dt, "layer_diagnostics"),
    standardize = attr(standardized_dt, "layer_diagnostics"),
    outputs = list(
      dataset_output_path = dataset_output_path,
      audit_output_path = audit_output_path
    )
  )

  attr(standardized_dt, "pipeline_diagnostics") <- diagnostics

  return(standardized_dt)
}

# backward-compatible alias
run_clean_harmonize_pipeline_batch <- run_post_processing_pipeline_batch

#' @title Run post-processing pipeline automatically
#' @description Runs post-processing when enabled and required objects exist.
#' @param auto_run Logical scalar auto-run flag.
#' @param env Environment for object resolution and assignment.
#' @return Invisibly returns post-processed dataset or `NULL`.
#' @importFrom checkmate assert_flag assert_environment
run_post_processing_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  raw_value <- get_required_object_or_null("fao_data_raw", env)
  config_value <- get_required_object_or_null("config", env)

  if (is.null(raw_value) || is.null(config_value)) {
    return(invisible(NULL))
  }

  post_processed_dt <- run_post_processing_pipeline_batch(
    raw_dt = raw_value,
    config = config_value,
    dataset_name = "fao_data_raw"
  )

  assign("fao_data_harmonized", post_processed_dt, envir = env)

  return(invisible(post_processed_dt))
}

# backward-compatible alias
run_clean_harmonize_pipeline_auto <- run_post_processing_pipeline_auto

run_post_processing_pipeline_auto(
  auto_run = isTRUE(getOption(
    "fao.run_post_processing_pipeline.auto",
    getOption("fao.run_clean_harmonize_pipeline.auto", TRUE)
  ))
)
