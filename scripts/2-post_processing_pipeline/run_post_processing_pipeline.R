# script: run post-processing pipeline
# description: deterministic clean + harmonize orchestration with centralized
# audit persistence under audit root.

#' @title Source one post-processing script
#' @description Sources one script with deterministic error handling.
#' @param script_path Character scalar script path.
#' @return Invisibly returns `TRUE`.
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
        "Failed while sourcing post-processing script",
        "x" = "script: {.path {script_path}}",
        "x" = "details: {error_condition$message}"
      ))
    }
  )
}

#' @title Source post-processing scripts
#' @description Sources clean + harmonize post-processing modules in deterministic order.
#' @param pipeline_root Character scalar script root path.
#' @return Invisibly returns `TRUE`.
source_post_processing_scripts <- function(
  pipeline_root = here::here("scripts", "2-post_processing_pipeline")
) {
  checkmate::assert_string(pipeline_root, min.chars = 1)

  script_names <- c(
    "21-post_processing_utilities.R",
    "22-clean_data.R",
    "24-harmonize_data.R",
    "25-post_processing_diagnostics.R"
  )

  purrr::walk(script_names, function(script_name) {
    source_post_processing_script(file.path(pipeline_root, script_name))
  })

  return(invisible(TRUE))
}

source_post_processing_scripts()

#' @title Get required object from environment or return `NULL`
#' @description Returns object value if available; warns and returns `NULL` otherwise.
#' @param object_name Character scalar object name.
#' @param env Environment.
#' @return Object value or `NULL`.
get_required_object_or_null <- function(object_name, env) {
  checkmate::assert_string(object_name, min.chars = 1)
  checkmate::assert_environment(env)

  if (!exists(object_name, envir = env, inherits = TRUE)) {
    cli::cli_warn("Automatic post-processing skipped: missing {.val {object_name}}")
    return(NULL)
  }

  return(get(object_name, envir = env, inherits = TRUE))
}

#' @title Persist post-processed dataset
#' @description Writes final dataset to configured processed exports path.
#' @param dataset_dt Post-processed dataset.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset name.
#' @return Character scalar output path.
persist_post_processed_dataset <- function(dataset_dt, config, dataset_name) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(config$paths$data$exports$processed, min.chars = 1)

  output_dir <- config$paths$data$exports$processed
  fs::dir_create(output_dir, recurse = TRUE)

  output_path <- file.path(output_dir, paste0(dataset_name, "_post_processed.xlsx"))

  openxlsx::write.xlsx(
    x = data.table::as.data.table(dataset_dt),
    file = output_path,
    overwrite = TRUE
  )

  return(output_path)
}

#' @title Run post-processing pipeline batch
#' @description Executes clean stage, harmonize stage, and centralized audit persistence.
#' @param raw_dt Input dataset.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset name.
#' @return Post-processed data table with `pipeline_diagnostics` attribute.
run_post_processing_pipeline_batch <- function(
  raw_dt,
  config,
  dataset_name = "fao_data_raw"
) {
  checkmate::assert_data_frame(raw_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  preflight <- collect_post_processing_preflight(
    config = config,
    dataset_columns = colnames(raw_dt)
  )
  assert_post_processing_preflight(preflight_result = preflight)

  execution_timestamp_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  clean_dt <- run_clean_data(
    dataset_dt = raw_dt,
    config = config,
    dataset_name = dataset_name,
    rewrite_template = FALSE
  )

  harmonized_dt <- run_harmonize_data(
    dataset_dt = clean_dt,
    config = config,
    dataset_name = dataset_name,
    rewrite_template = FALSE
  )

  clean_audit <- attr(clean_dt, "layer_audit")
  harmonize_audit <- attr(harmonized_dt, "layer_audit")

  clean_audit_path <- persist_stage_audit_workbook(
    stage_audit_dt = clean_audit,
    config = config,
    stage = "clean",
    dataset_name = dataset_name,
    execution_timestamp_utc = execution_timestamp_utc
  )

  harmonize_audit_path <- persist_stage_audit_workbook(
    stage_audit_dt = harmonize_audit,
    config = config,
    stage = "harmonize",
    dataset_name = dataset_name,
    execution_timestamp_utc = execution_timestamp_utc
  )

  diagnostics_path <- persist_post_processing_diagnostics(
    clean_audit_dt = clean_audit,
    harmonize_audit_dt = harmonize_audit,
    config = config,
    dataset_name = dataset_name,
    execution_timestamp_utc = execution_timestamp_utc
  )

  dataset_output_path <- persist_post_processed_dataset(
    dataset_dt = harmonized_dt,
    config = config,
    dataset_name = dataset_name
  )

  pipeline_diagnostics <- list(
    clean = attr(clean_dt, "layer_diagnostics"),
    harmonize = attr(harmonized_dt, "layer_diagnostics"),
    outputs = list(
      clean_audit_path = clean_audit_path,
      harmonize_audit_path = harmonize_audit_path,
      diagnostics_path = diagnostics_path,
      dataset_output_path = dataset_output_path
    )
  )

  attr(harmonized_dt, "pipeline_diagnostics") <- pipeline_diagnostics

  return(harmonized_dt)
}

# backward-compatible alias
run_clean_harmonize_pipeline_batch <- run_post_processing_pipeline_batch

#' @title Run post-processing pipeline automatically
#' @description Executes batch pipeline when auto-run is enabled.
#' @param auto_run Logical scalar auto-run flag.
#' @param env Environment.
#' @return Invisibly returns output dataset or `NULL`.
run_post_processing_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  raw_dt <- get_required_object_or_null(object_name = "fao_data_raw", env = env)
  config <- get_required_object_or_null(object_name = "config", env = env)

  if (is.null(raw_dt) || is.null(config)) {
    return(invisible(NULL))
  }

  fao_data_harmonized <- run_post_processing_pipeline_batch(
    raw_dt = raw_dt,
    config = config,
    dataset_name = "fao_data_raw"
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
