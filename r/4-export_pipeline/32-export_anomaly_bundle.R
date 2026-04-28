# script: 32-export_anomaly_bundle.R
# description: export validated anomaly analytical objects into one deterministic
# workbook with one sheet per object.

#' @title Collect anomaly bundle for export
#' @param env Environment used to detect anomaly bundle object.
#' @return Named anomaly bundle list.
#' @importFrom checkmate assert_environment
collect_anomaly_bundle_for_export <- function(env = .GlobalEnv) {
  checkmate::assert_environment(env)

  bundle_name <- get_pipeline_constants()$object_names$anomaly_bundle

  if (!exists(bundle_name, envir = env, inherits = TRUE)) {
    cli::cli_abort(
      "missing anomaly bundle object for export: {.val {bundle_name}}"
    )
  }

  bundle_value <- get(bundle_name, envir = env, inherits = TRUE)
  assert_anomaly_bundle_export_contract(bundle_value)

  return(bundle_value)
}

#' @title Build deterministic anomaly export path
#' @param config Named configuration list.
#' @return Character scalar workbook path.
#' @importFrom checkmate assert_list
build_anomaly_export_path <- function(config) {
  checkmate::assert_list(config, names = "named")

  processed_dir <- get_config_string(
    config = config,
    path = c("paths", "data", "export", "processed"),
    field_name = "config$paths$data$export$processed"
  )

  return(fs::path(here::here(processed_dir), "anomaly_bundle.xlsx"))
}

#' @title Export anomaly bundle as one workbook
#' @param config Named configuration list.
#' @param anomaly_bundle Named anomaly bundle.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar workbook path.
#' @importFrom checkmate assert_list assert_flag
export_anomaly_bundle <- function(config, anomaly_bundle, overwrite = TRUE) {
  checkmate::assert_list(config, names = "named")
  checkmate::assert_flag(overwrite)
  assert_anomaly_bundle_export_contract(anomaly_bundle)

  output_path <- build_anomaly_export_path(config)

  if (!overwrite && file.exists(output_path)) {
    cli::cli_abort(
      "file already exists and overwrite is disabled: {.path {output_path}}"
    )
  }

  workbook_payload <- list(
    tendencies = data.table::as.data.table(anomaly_bundle$tendencies$data),
    outliers = data.table::as.data.table(anomaly_bundle$outliers$data),
    anomalies = data.table::as.data.table(anomaly_bundle$anomalies$data),
    diagnostics = data.table::as.data.table(anomaly_bundle$diagnostics$data)
  )

  writexl::write_xlsx(workbook_payload, path = output_path)

  return(output_path)
}


#' @title Assert anomaly bundle export contract
#' @param anomaly_bundle Named list bundle.
#' @return Invisible TRUE when valid.
assert_anomaly_bundle_export_contract <- function(anomaly_bundle) {
  checkmate::assert_list(anomaly_bundle, names = "named")
  checkmate::assert_names(
    names(anomaly_bundle),
    must.include = c("tendencies", "outliers", "anomalies", "diagnostics")
  )

  checkmate::assert_list(anomaly_bundle$tendencies, names = "named")
  checkmate::assert_list(anomaly_bundle$outliers, names = "named")
  checkmate::assert_list(anomaly_bundle$anomalies, names = "named")
  checkmate::assert_list(anomaly_bundle$diagnostics, names = "named")

  checkmate::assert_names(
    names(anomaly_bundle$tendencies),
    must.include = c("schema_version", "object_type", "generated_at_utc", "metadata", "data")
  )
  checkmate::assert_names(
    names(anomaly_bundle$outliers),
    must.include = c("schema_version", "object_type", "generated_at_utc", "metadata", "data")
  )
  checkmate::assert_names(
    names(anomaly_bundle$anomalies),
    must.include = c("schema_version", "object_type", "generated_at_utc", "metadata", "data")
  )
  checkmate::assert_names(
    names(anomaly_bundle$diagnostics),
    must.include = c("schema_version", "object_type", "generated_at_utc", "metadata", "data")
  )

  checkmate::assert_data_table(anomaly_bundle$tendencies$data)
  checkmate::assert_data_table(anomaly_bundle$outliers$data)
  checkmate::assert_data_table(anomaly_bundle$anomalies$data)
  checkmate::assert_data_table(anomaly_bundle$diagnostics$data)

  return(invisible(TRUE))
}
