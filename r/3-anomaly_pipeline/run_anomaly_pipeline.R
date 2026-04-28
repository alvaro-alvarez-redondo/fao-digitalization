# script: run_anomaly_pipeline.R
# description: source anomaly components and run deterministic tendencies,
# outliers, anomalies, and diagnostics computation from harmonized data.

if (!exists("get_pipeline_constants", mode = "function", inherits = TRUE)) {
  source(
    here::here("r", "0-general_pipeline", "01-setup.R"),
    echo = FALSE
  )
}

anomaly_scripts <- c(
  "30-anomaly_objects.R",
  "31-anomaly_computation.R"
)

purrr::walk(
  anomaly_scripts,
  \(script_name) {
    source(
      here::here("r", "3-anomaly_pipeline", script_name),
      echo = FALSE
    )
  }
)

#' @title Run anomaly pipeline
#' @description Computes tendencies, outliers, anomalies, and diagnostics from
#' harmonized data and returns validated analytical objects.
#' @param config Named configuration list.
#' @param harmonize_dt Harmonized input data table.
#' @return Named list with analytical S3 objects.
run_anomaly_pipeline <- function(config, harmonize_dt) {
  checkmate::assert_list(config, names = "named")
  checkmate::assert_data_frame(harmonize_dt, min.rows = 0)

  anomaly_config <- resolve_anomaly_config(config)

  source_dt <- prepare_anomaly_source(
    harmonize_dt = harmonize_dt,
    anomaly_config = anomaly_config
  )

  tendencies_payload <- compute_tendencies_table(
    source_dt = source_dt,
    anomaly_config = anomaly_config
  )

  outlier_payload <- compute_outliers_table(
    source_dt = source_dt,
    anomaly_config = anomaly_config
  )

  anomaly_payload <- compute_anomalies_table(
    tendencies_dt = tendencies_payload$tendencies,
    anomaly_config = anomaly_config
  )

  diagnostics_dt <- build_diagnostics_table(
    tendency_diag = tendencies_payload$diagnostics,
    outlier_diag = outlier_payload$diagnostics,
    anomaly_diag = anomaly_payload$diagnostics,
    group_columns = anomaly_config$group_by
  )

  metadata <- list(
    input_object = get_pipeline_constants()$object_names$harmonize,
    group_by = anomaly_config$group_by,
    value_column = anomaly_config$value_column,
    year_column = anomaly_config$year_column,
    time_key_column = anomaly_config$time_key_column,
    thresholds = list(
      iqr_multiplier = anomaly_config$iqr_multiplier,
      mad_z_threshold = anomaly_config$mad_z_threshold,
      mad_scale_constant = anomaly_config$mad_scale_constant,
      min_group_size = anomaly_config$min_group_size,
      boundary_tolerance = anomaly_config$boundary_tolerance
    )
  )

  object_bundle <- list(
    tendencies = new_whep_tendencies(
      data_dt = tendencies_payload$tendencies,
      metadata = metadata,
      schema_version = anomaly_config$schema_version,
      generated_at_utc = anomaly_config$generated_at_utc
    ),
    outliers = new_whep_outliers(
      data_dt = outlier_payload$outliers,
      metadata = metadata,
      schema_version = anomaly_config$schema_version,
      generated_at_utc = anomaly_config$generated_at_utc
    ),
    anomalies = new_whep_anomalies(
      data_dt = anomaly_payload$anomalies,
      metadata = metadata,
      schema_version = anomaly_config$schema_version,
      generated_at_utc = anomaly_config$generated_at_utc
    ),
    diagnostics = new_whep_anomaly_diagnostics(
      data_dt = diagnostics_dt,
      metadata = metadata,
      schema_version = anomaly_config$schema_version,
      generated_at_utc = anomaly_config$generated_at_utc
    )
  )

  validate_whep_tendencies(object_bundle$tendencies)
  validate_whep_outliers(object_bundle$outliers)
  validate_whep_anomalies(object_bundle$anomalies)
  validate_whep_anomaly_diagnostics(object_bundle$diagnostics)

  assert_anomaly_pipeline_contract(object_bundle)

  return(object_bundle)
}

#' @title Assert anomaly pipeline return contract
#' @param anomaly_bundle Named list bundle to validate.
#' @return Invisible TRUE when valid.
assert_anomaly_pipeline_contract <- function(anomaly_bundle) {
  checkmate::assert_list(anomaly_bundle, names = "named")
  checkmate::assert_names(
    names(anomaly_bundle),
    must.include = c("tendencies", "outliers", "anomalies", "diagnostics")
  )

  validate_whep_tendencies(anomaly_bundle$tendencies)
  validate_whep_outliers(anomaly_bundle$outliers)
  validate_whep_anomalies(anomaly_bundle$anomalies)
  validate_whep_anomaly_diagnostics(anomaly_bundle$diagnostics)

  return(invisible(TRUE))
}

#' @title Run anomaly pipeline automatically
#' @description Executes anomaly stage when automatic mode is enabled and
#' required upstream objects exist.
#' @param auto_run Logical scalar controlling automatic execution.
#' @param env Environment for object resolution and assignment.
#' @return Invisible anomaly bundle or NULL.
run_anomaly_pipeline_auto <- function(auto_run, env = .GlobalEnv) {
  checkmate::assert_flag(auto_run)
  checkmate::assert_environment(env)

  if (!isTRUE(auto_run)) {
    return(invisible(NULL))
  }

  if (!exists("config", envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic anomaly pipeline skipped: missing {.val config} in environment"
    )
    return(invisible(NULL))
  }

  harmonize_name <- get_pipeline_constants()$object_names$harmonize

  if (!exists(harmonize_name, envir = env, inherits = TRUE)) {
    cli::cli_warn(
      "automatic anomaly pipeline skipped: missing {.val {harmonize_name}} in environment"
    )
    return(invisible(NULL))
  }

  anomaly_bundle <- run_anomaly_pipeline(
    config = get("config", envir = env, inherits = TRUE),
    harmonize_dt = get(harmonize_name, envir = env, inherits = TRUE)
  )

  assignment_values <- list(
    anomaly_bundle,
    anomaly_bundle$tendencies,
    anomaly_bundle$outliers,
    anomaly_bundle$anomalies,
    anomaly_bundle$diagnostics
  )
  names(assignment_values) <- c(
    get_pipeline_constants()$object_names$anomaly_bundle,
    get_pipeline_constants()$object_names$tendencies,
    get_pipeline_constants()$object_names$outliers,
    get_pipeline_constants()$object_names$anomalies,
    get_pipeline_constants()$object_names$anomaly_diagnostics
  )

  assign_environment_values(values = assignment_values, env = env)

  return(invisible(anomaly_bundle))
}

run_anomaly_pipeline_auto(
  auto_run = isTRUE(getOption(
    get_pipeline_constants()$auto_run_options$anomaly,
    TRUE
  ))
)
