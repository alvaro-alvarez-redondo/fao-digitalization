# script: run post-processing pipeline
# description: source post-processing scripts and execute deterministic clean and
# harmonize stages with structured audit persistence.

if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(
    here::here("scripts", "0-general_pipeline", "01-setup.R"),
    echo = FALSE
  )
}


#' @title Source one post-processing script
#' @description Sources a single script with deterministic error handling.
#' @param script_path Character scalar script path.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_string
source_post_processing_script <- function(script_path) {
  checkmate::assert_string(script_path, min.chars = 1)

  if (!file.exists(script_path)) {
    cli::cli_abort(
      "Required post-processing script not found: {.path {script_path}}"
    )
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
#' @description Sources required scripts for clean and harmonize workflow.
#' @param pipeline_root Character scalar path to post-processing script folder.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_string
source_post_processing_scripts <- function(
  pipeline_root = here::here("scripts", "2-post_processing_pipeline")
) {
  checkmate::assert_string(pipeline_root, min.chars = 1)

  script_names <- c(
    "20-data_audit.R",
    "21-post_processing_utilities.R",
    "23-post_processing_rule_engine.R",
    "22-clean_harmonize_data.R",
    "24-standardize_units.R",
    "25-post_processing_diagnostics.R"
  )

  purrr::walk(script_names, function(script_name) {
    source_post_processing_script(fs::path(pipeline_root, script_name))
  })

  return(invisible(TRUE))
}

source_post_processing_scripts()

#' @title Resolve units standardization runner
#' @description Resolves the first available units standardization function from
#' supported current and legacy symbol names using an ordered candidate vector.
#' @return Function implementing units standardization.
resolve_units_standardization_runner <- function() {
  runner_candidates <- c(
    "run_standardize_units_layer_batch",
    "run_number_standardization_layer_batch",
    "run_number_standarization_layer_batch",
    "run_number_harmonization_layer_batch"
  )

  available_runner <- runner_candidates[
    vapply(
      runner_candidates,
      exists,
      logical(1),
      mode = "function",
      inherits = TRUE
    )
  ][1]

  if (is.na(available_runner)) {
    cli::cli_abort(
      "No units standardization runner found. Expected {.fn run_standardize_units_layer_batch}, {.fn run_number_standardization_layer_batch}, {.fn run_number_standarization_layer_batch}, or {.fn run_number_harmonization_layer_batch}."
    )
  }

  return(get(
    available_runner,
    mode = "function",
    inherits = TRUE
  ))
}

#' @title Run units standardization stage
#' @description Executes the resolved units standardization implementation using
#' the cleaned dataset and pipeline configuration.
#' @param cleaned_dt Cleaned dataset to standardize.
#' @param config Named configuration list.
#' @return Standardized dataset returned by the resolved runner.
#' @importFrom checkmate assert_data_frame assert_list
run_units_standardization_stage <- function(cleaned_dt, config) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  units_standardization_runner <- resolve_units_standardization_runner()

  return(units_standardization_runner(
    cleaned_dt = cleaned_dt,
    config = config
  ))
}

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

#' @title Run post-processing pipeline batch
#' @description Runs deterministic preflight, clean stage, units standardization stage,
#' harmonize stage, and persistence of dataset and audit artifacts.
#' @param raw_dt Raw dataset.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Post-processed `data.table` with `pipeline_diagnostics` attribute.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @importFrom progressr with_progress progressor
run_post_processing_pipeline_batch <- function(
  raw_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(raw_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  total_steps <- 9

  return(progressr::with_progress({
    progress <- progressr::progressor(steps = total_steps)

    progress("Post-Processing Pipeline Progress: auditing raw data")
    audited_raw_dt <- audit_data_output(
      dataset_dt = raw_dt,
      config = config
    )

    progress(
      "Post-Processing Pipeline Progress: initializing audit directories"
    )
    audit_paths <- initialize_post_processing_audit_root(config)

    progress("Post-Processing Pipeline Progress: generating rule templates")
    template_paths <- generate_post_processing_rule_templates(
      config = config,
      overwrite = TRUE
    )

    progress("Post-Processing Pipeline Progress: collecting preflight checks")
    preflight_result <- collect_post_processing_preflight(
      config = config,
      dataset_columns = colnames(audited_raw_dt),
      expected_columns = colnames(audited_raw_dt)
    )

    progress("Post-Processing Pipeline Progress: asserting preflight checks")
    assert_post_processing_preflight(preflight_result)

    execution_timestamp_utc <- format(
      Sys.time(),
      get_pipeline_constants()$timestamp_format_utc,
      tz = "UTC"
    )

    progress("Post-Processing Pipeline Progress: running clean layer")
    cleaned_dt <- run_cleaning_layer_batch(
      dataset_dt = audited_raw_dt,
      config = config,
      dataset_name = dataset_name
    )

    progress("Post-Processing Pipeline Progress: running standardize layer")
    normalized_dt <- run_units_standardization_stage(
      cleaned_dt = cleaned_dt,
      config = config
    )

    progress("Post-Processing Pipeline Progress: running harmonize layer")
    harmonized_dt <- run_harmonize_layer_batch(
      dataset_dt = normalized_dt,
      config = config,
      dataset_name = dataset_name
    )

    clean_audit <- attr(cleaned_dt, "layer_audit")
    harmonize_audit <- attr(harmonized_dt, "layer_audit")

    progress("Post-Processing Pipeline Progress: persisting diagnostics")
    audit_output_path <- persist_post_processing_audit(
      clean_audit_dt = clean_audit,
      harmonize_audit_dt = harmonize_audit,
      standardize_diagnostics = attr(normalized_dt, "layer_diagnostics"),
      dataset_name = dataset_name,
      execution_timestamp_utc = execution_timestamp_utc,
      config = config
    )

    diagnostics <- list(
      clean = attr(cleaned_dt, "layer_diagnostics"),
      standardize_units = attr(normalized_dt, "layer_diagnostics"),
      harmonize = attr(harmonized_dt, "layer_diagnostics"),
      outputs = list(
        audit_output_path = audit_output_path,
        audit_root_dir = audit_paths$audit_root_dir,
        diagnostics_dir = audit_paths$diagnostics_dir,
        templates_dir = audit_paths$templates_dir,
        rules_template_path = template_paths[["rules_template"]],
        data_audit_output_path = config$paths$data$audit$audit_file_path
      )
    )

    attr(harmonized_dt, "pipeline_diagnostics") <- diagnostics
    attr(harmonized_dt, "stage_cleaned") <- cleaned_dt
    attr(harmonized_dt, "stage_normalized") <- normalized_dt

    return(harmonized_dt)
  }))
}

# backward-compatible alias
run_clean_harmonize_pipeline_batch <- run_post_processing_pipeline_batch

# backward-compatible wrapper for legacy symbol
#' @title Run clean data (legacy wrapper)
#' @description Backward-compatible wrapper that delegates to
#' `run_cleaning_layer_batch`.
#' @param dataset_dt Dataset to clean.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Cleaned dataset returned by `run_cleaning_layer_batch`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_clean_data <- function(
  dataset_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  return(run_cleaning_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    dataset_name = dataset_name
  ))
}

# backward-compatible wrapper for legacy symbol
#' @title Run harmonize data (legacy wrapper)
#' @description Backward-compatible wrapper that delegates to
#' `run_harmonize_layer_batch`.
#' @param dataset_dt Dataset to harmonize.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset identifier.
#' @return Harmonized dataset returned by `run_harmonize_layer_batch`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
run_harmonize_data <- function(
  dataset_dt,
  config,
  dataset_name = get_pipeline_constants()$dataset_default_name
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)

  return(run_harmonize_layer_batch(
    dataset_dt = dataset_dt,
    config = config,
    dataset_name = dataset_name
  ))
}

#' @title Run standardize units data (legacy wrapper)
#' @description Backward-compatible wrapper that delegates to
#' `run_units_standardization_stage`.
#' @param cleaned_dt Cleaned dataset.
#' @param config Named configuration list.
#' @return Standardized dataset returned by `run_units_standardization_stage`.
#' @importFrom checkmate assert_data_frame assert_list
run_standardize_units_data <- function(cleaned_dt, config) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  return(run_units_standardization_stage(
    cleaned_dt = cleaned_dt,
    config = config
  ))
}

# backward-compatible wrapper for legacy symbol
#' @title Persist stage audit workbook (legacy wrapper)
#' @description Backward-compatible wrapper that delegates to
#' `persist_post_processing_audit`.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_diagnostics List of units standardization diagnostics.
#' @param dataset_name Character scalar dataset identifier.
#' @param execution_timestamp_utc Character scalar UTC timestamp.
#' @param config Named configuration list.
#' @return Output path returned by `persist_post_processing_audit`.
#' @importFrom checkmate assert_data_frame assert_list assert_string
persist_stage_audit_workbook <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  standardize_diagnostics = list(),
  dataset_name,
  execution_timestamp_utc,
  config
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_list(standardize_diagnostics)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)
  checkmate::assert_list(config, min.len = 1)

  return(persist_post_processing_audit(
    clean_audit_dt = clean_audit_dt,
    harmonize_audit_dt = harmonize_audit_dt,
    standardize_diagnostics = standardize_diagnostics,
    dataset_name = dataset_name,
    execution_timestamp_utc = execution_timestamp_utc,
    config = config
  ))
}

# backward-compatible wrapper for legacy symbol
persist_post_processing_diagnostics <- persist_stage_audit_workbook

#' @title Run post-processing pipeline automatically
#' @description Runs post-processing when enabled and required objects exist.
#' @param auto_run Logical scalar auto-run flag.
#' @param env Environment for object resolution and assignment.
#' @return Invisibly returns post-processed dataset or `NULL`.
#' @importFrom checkmate assert_flag assert_environment
run_post_processing_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  pipeline_constants <- get_pipeline_constants()

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  raw_value <- get_required_object_or_null(
    pipeline_constants$object_names$raw,
    env
  )
  config_value <- get_required_object_or_null("config", env)

  if (is.null(raw_value) || is.null(config_value)) {
    return(invisible(NULL))
  }

  harmonized_dt <- run_post_processing_pipeline_batch(
    raw_dt = raw_value,
    config = config_value,
    dataset_name = pipeline_constants$dataset_default_name
  )

  assignment_helper <- pipeline_constants$helper_requirements$assignment_helper

  if (!exists(assignment_helper, mode = "function", inherits = TRUE)) {
    cli::cli_abort(
      "missing shared helper {.fn {assignment_helper}}; source {.file {pipeline_constants$helper_requirements$assignment_helper_source}}"
    )
  }

  assignment_values <- list(
    attr(harmonized_dt, "stage_cleaned"),
    attr(harmonized_dt, "stage_normalized"),
    harmonized_dt
  )
  names(assignment_values) <- c(
    pipeline_constants$object_names$cleaned,
    pipeline_constants$object_names$normalized,
    pipeline_constants$object_names$harmonized
  )

  assign_environment_values(values = assignment_values, env = env)

  return(invisible(harmonized_dt))
}

# backward-compatible alias
run_clean_harmonize_pipeline_auto <- run_post_processing_pipeline_auto

run_post_processing_pipeline_auto(
  auto_run = isTRUE(getOption(
    get_pipeline_constants()$auto_run_options$post_processing,
    getOption(
      get_pipeline_constants()$auto_run_options$post_processing_legacy,
      TRUE
    )
  ))
)
