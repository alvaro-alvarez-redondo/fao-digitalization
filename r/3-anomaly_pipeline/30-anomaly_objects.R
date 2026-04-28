# script: 30-anomaly_objects.R
# description: defines S3 analytical-object constructors and validators for
# tendencies, outliers, anomalies, and diagnostics payloads.

#' @title Build analytical object envelope
#' @param data_dt Data table payload.
#' @param metadata Named list metadata.
#' @param object_type Character scalar object type.
#' @param class_name Character scalar S3 class.
#' @param schema_version Character schema version.
#' @param generated_at_utc Character UTC timestamp string.
#' @return Named list with analytical object contract.
#' @importFrom checkmate assert_data_table assert_list assert_string
new_whep_analytical_object <- function(
  data_dt,
  metadata,
  object_type,
  class_name,
  schema_version = "1.0.0",
  generated_at_utc = "1970-01-01T00:00:00Z"
) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_list(metadata, names = "named")
  checkmate::assert_string(object_type, min.chars = 1)
  checkmate::assert_string(class_name, min.chars = 1)
  checkmate::assert_string(schema_version, min.chars = 1)
  checkmate::assert_string(generated_at_utc, min.chars = 1)

  object_value <- list(
    schema_version = schema_version,
    object_type = object_type,
    generated_at_utc = generated_at_utc,
    metadata = metadata,
    data = data.table::copy(data_dt)
  )

  class(object_value) <- c(class_name, "whep_analytical_object", "list")

  return(object_value)
}

#' @title Build tendencies analytical object
#' @param data_dt Data table with tendencies rows.
#' @param metadata Named list metadata.
#' @param schema_version Character schema version.
#' @param generated_at_utc Character UTC timestamp string.
#' @return S3 object of class `whep_tendencies`.
new_whep_tendencies <- function(
  data_dt,
  metadata,
  schema_version = "1.0.0",
  generated_at_utc = "1970-01-01T00:00:00Z"
) {
  object_value <- new_whep_analytical_object(
    data_dt = data_dt,
    metadata = metadata,
    object_type = "tendencies",
    class_name = "whep_tendencies",
    schema_version = schema_version,
    generated_at_utc = generated_at_utc
  )

  validate_whep_tendencies(object_value)

  return(object_value)
}

#' @title Build outliers analytical object
#' @param data_dt Data table with outliers rows.
#' @param metadata Named list metadata.
#' @param schema_version Character schema version.
#' @param generated_at_utc Character UTC timestamp string.
#' @return S3 object of class `whep_outliers`.
new_whep_outliers <- function(
  data_dt,
  metadata,
  schema_version = "1.0.0",
  generated_at_utc = "1970-01-01T00:00:00Z"
) {
  object_value <- new_whep_analytical_object(
    data_dt = data_dt,
    metadata = metadata,
    object_type = "outliers",
    class_name = "whep_outliers",
    schema_version = schema_version,
    generated_at_utc = generated_at_utc
  )

  validate_whep_outliers(object_value)

  return(object_value)
}

#' @title Build anomalies analytical object
#' @param data_dt Data table with anomalies rows.
#' @param metadata Named list metadata.
#' @param schema_version Character schema version.
#' @param generated_at_utc Character UTC timestamp string.
#' @return S3 object of class `whep_anomalies`.
new_whep_anomalies <- function(
  data_dt,
  metadata,
  schema_version = "1.0.0",
  generated_at_utc = "1970-01-01T00:00:00Z"
) {
  object_value <- new_whep_analytical_object(
    data_dt = data_dt,
    metadata = metadata,
    object_type = "anomalies",
    class_name = "whep_anomalies",
    schema_version = schema_version,
    generated_at_utc = generated_at_utc
  )

  validate_whep_anomalies(object_value)

  return(object_value)
}

#' @title Build diagnostics analytical object
#' @param data_dt Data table with diagnostics rows.
#' @param metadata Named list metadata.
#' @param schema_version Character schema version.
#' @param generated_at_utc Character UTC timestamp string.
#' @return S3 object of class `whep_anomaly_diagnostics`.
new_whep_anomaly_diagnostics <- function(
  data_dt,
  metadata,
  schema_version = "1.0.0",
  generated_at_utc = "1970-01-01T00:00:00Z"
) {
  object_value <- new_whep_analytical_object(
    data_dt = data_dt,
    metadata = metadata,
    object_type = "diagnostics",
    class_name = "whep_anomaly_diagnostics",
    schema_version = schema_version,
    generated_at_utc = generated_at_utc
  )

  validate_whep_anomaly_diagnostics(object_value)

  return(object_value)
}

#' @title Validate shared analytical object contract
#' @param analytical_object Named list object.
#' @return Invisible TRUE when valid.
validate_whep_analytical_object <- function(analytical_object) {
  checkmate::assert_list(analytical_object, names = "named")
  checkmate::assert_names(
    names(analytical_object),
    must.include = c(
      "schema_version",
      "object_type",
      "generated_at_utc",
      "metadata",
      "data"
    )
  )
  checkmate::assert_string(analytical_object$schema_version, min.chars = 1)
  checkmate::assert_string(analytical_object$object_type, min.chars = 1)
  checkmate::assert_string(analytical_object$generated_at_utc, min.chars = 1)
  checkmate::assert_list(analytical_object$metadata, names = "named")
  checkmate::assert_data_table(analytical_object$data)

  return(invisible(TRUE))
}

validate_whep_tendencies <- function(analytical_object) {
  validate_whep_analytical_object(analytical_object)
  if (!inherits(analytical_object, "whep_tendencies")) {
    cli::cli_abort("object is not class {.val whep_tendencies}")
  }

  return(invisible(TRUE))
}

validate_whep_outliers <- function(analytical_object) {
  validate_whep_analytical_object(analytical_object)
  if (!inherits(analytical_object, "whep_outliers")) {
    cli::cli_abort("object is not class {.val whep_outliers}")
  }

  return(invisible(TRUE))
}

validate_whep_anomalies <- function(analytical_object) {
  validate_whep_analytical_object(analytical_object)
  if (!inherits(analytical_object, "whep_anomalies")) {
    cli::cli_abort("object is not class {.val whep_anomalies}")
  }

  return(invisible(TRUE))
}

validate_whep_anomaly_diagnostics <- function(analytical_object) {
  validate_whep_analytical_object(analytical_object)
  if (!inherits(analytical_object, "whep_anomaly_diagnostics")) {
    cli::cli_abort("object is not class {.val whep_anomaly_diagnostics}")
  }

  return(invisible(TRUE))
}
