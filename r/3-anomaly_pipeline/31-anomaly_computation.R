# script: 31-anomaly_computation.R
# description: deterministic computation functions for tendencies, outliers,
# anomalies, and diagnostics tables from harmonized data.

#' @title Resolve anomaly pipeline configuration
#' @param config Named list configuration.
#' @return Normalized anomaly configuration list.
resolve_anomaly_config <- function(config) {
  checkmate::assert_list(config, names = "named")

  anomaly_config <- config$anomaly_config

  if (is.null(anomaly_config)) {
    cli::cli_abort("missing {.val config$anomaly_config}")
  }

  checkmate::assert_list(anomaly_config, names = "named")
  checkmate::assert_character(
    anomaly_config$group_by,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE
  )
  checkmate::assert_string(anomaly_config$value_column, min.chars = 1)
  checkmate::assert_string(anomaly_config$year_column, min.chars = 1)
  checkmate::assert_string(anomaly_config$time_key_column, min.chars = 1)
  checkmate::assert_flag(anomaly_config$include_mean)
  checkmate::assert_number(anomaly_config$iqr_multiplier, lower = 0)
  checkmate::assert_number(anomaly_config$mad_z_threshold, lower = 0)
  checkmate::assert_number(anomaly_config$mad_scale_constant, lower = 0)
  checkmate::assert_int(anomaly_config$min_group_size, lower = 2)
  checkmate::assert_number(anomaly_config$boundary_tolerance, lower = 0)
  checkmate::assert_string(anomaly_config$schema_version, min.chars = 1)
  checkmate::assert_string(anomaly_config$generated_at_utc, min.chars = 1)

  return(anomaly_config)
}

#' @title Assert anomaly input contract
#' @param data_dt Input data table.
#' @param anomaly_config Resolved anomaly configuration.
#' @return Invisibly TRUE when valid.
assert_anomaly_input_contract <- function(data_dt, anomaly_config) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_list(anomaly_config, names = "named")

  required_columns <- unique(c(
    anomaly_config$group_by,
    anomaly_config$year_column,
    anomaly_config$value_column
  ))

  validate_required_columns(
    data_dt,
    required_columns = required_columns,
    context = "input_preprocessing_gate"
  )

  return(invisible(TRUE))
}

#' @title Build deterministic temporal key column
#' @param data_dt Input data table.
#' @param year_column Character year column name.
#' @param optional_time_column Character optional secondary key column.
#' @return Data table with temporal columns.
build_temporal_key <- function(data_dt, year_column, optional_time_column) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_string(year_column, min.chars = 1)
  checkmate::assert_string(optional_time_column, min.chars = 1)

  temporal_year <- suppressWarnings(as.integer(as.character(data_dt[[year_column]])))

  if (optional_time_column %in% names(data_dt)) {
    temporal_sub_key <- as.character(data_dt[[optional_time_column]])
  } else {
    temporal_sub_key <- rep("", nrow(data_dt))
  }

  data_dt[, `:=`(
    ..temporal_year = temporal_year,
    ..temporal_sub_key = temporal_sub_key,
    ..temporal_key = paste(as.character(get(year_column)), temporal_sub_key, sep = "|"),
    ..row_id = .I
  )]

  return(data_dt)
}

#' @title Prepare anomaly source data deterministically
#' @param harmonize_dt Input harmonize table.
#' @param anomaly_config Resolved anomaly configuration.
#' @return Deterministically sorted data table.
prepare_anomaly_source <- function(harmonize_dt, anomaly_config) {
  source_dt <- data.table::as.data.table(harmonize_dt)

  assert_anomaly_input_contract(source_dt, anomaly_config)

  source_dt <- build_temporal_key(
    data_dt = source_dt,
    year_column = anomaly_config$year_column,
    optional_time_column = anomaly_config$time_key_column
  )

  value_column <- anomaly_config$value_column
  source_dt[, (value_column) := as.numeric(get(value_column))]

  sort_columns <- c(
    anomaly_config$group_by,
    "..temporal_year",
    "..temporal_sub_key",
    "..row_id"
  )

  data.table::setorderv(source_dt, sort_columns, na.last = TRUE)

  return(source_dt)
}

#' @title Compute grouped tendencies from raw values
#' @param source_dt Source harmonize table.
#' @param anomaly_config Resolved anomaly configuration.
#' @return Named list with tendencies and diagnostics tables.
compute_tendencies_table <- function(source_dt, anomaly_config) {
  group_columns <- anomaly_config$group_by
  value_column <- anomaly_config$value_column

  metrics_by_group <- source_dt[
    ,
    .(
      observations = .N,
      median_value = stats::median(get(value_column), na.rm = TRUE),
      mean_value = mean(get(value_column), na.rm = TRUE)
    ),
    by = c(group_columns, "..temporal_year", "..temporal_sub_key", "..temporal_key")
  ]

  if (!isTRUE(anomaly_config$include_mean)) {
    metrics_by_group[, mean_value := NULL]
  }

  data.table::setorderv(
    metrics_by_group,
    c(group_columns, "..temporal_year", "..temporal_sub_key"),
    na.last = TRUE
  )

  metrics_by_group[, previous_value := data.table::shift(median_value), by = group_columns]
  metrics_by_group[, absolute_change := median_value - previous_value]
  metrics_by_group[, percent_change := ifelse(
    is.na(previous_value) | previous_value == 0,
    NA_real_,
    ((median_value - previous_value) / previous_value) * 100
  )]

  metrics_by_group[, change_status := data.table::fifelse(
    is.na(previous_value),
    "undefined_pct_change_missing_previous",
    data.table::fifelse(
      previous_value == 0,
      "undefined_pct_change_zero_previous",
      "successful_computation"
    )
  )]

  tendency_diagnostics <- metrics_by_group[
    ,
    .(
      successful_computation_count = sum(change_status == "successful_computation"),
      undefined_pct_change_count = sum(change_status != "successful_computation"),
      observations_total = sum(observations)
    ),
    by = group_columns
  ]

  return(list(
    tendencies = metrics_by_group[],
    diagnostics = tendency_diagnostics[]
  ))
}

#' @title Compute IQR outliers from raw values
#' @param source_dt Source harmonize table.
#' @param anomaly_config Resolved anomaly configuration.
#' @return Named list with outliers and diagnostics tables.
compute_outliers_table <- function(source_dt, anomaly_config) {
  group_columns <- anomaly_config$group_by
  value_column <- anomaly_config$value_column
  boundary_tolerance <- anomaly_config$boundary_tolerance

  scored_dt <- source_dt[, {
    q1 <- stats::quantile(get(value_column), probs = 0.25, na.rm = TRUE, names = FALSE, type = 7)
    q3 <- stats::quantile(get(value_column), probs = 0.75, na.rm = TRUE, names = FALSE, type = 7)
    iqr_value <- q3 - q1
    lower_bound <- q1 - (anomaly_config$iqr_multiplier * iqr_value)
    upper_bound <- q3 + (anomaly_config$iqr_multiplier * iqr_value)
    value_vector <- get(value_column)

    .SD[, `:=`(
      iqr_q1 = q1,
      iqr_q3 = q3,
      iqr_value = iqr_value,
      iqr_lower_bound = lower_bound,
      iqr_upper_bound = upper_bound,
      is_outlier = value_vector < lower_bound | value_vector > upper_bound,
      is_boundary_condition = abs(value_vector - lower_bound) <= boundary_tolerance |
        abs(value_vector - upper_bound) <= boundary_tolerance
    )]
  }, by = group_columns]

  outlier_dt <- scored_dt[is_outlier == TRUE]
  data.table::setorderv(
    outlier_dt,
    c(group_columns, "..temporal_year", "..temporal_sub_key", "..row_id"),
    na.last = TRUE
  )

  outlier_diagnostics <- scored_dt[
    ,
    .(
      successful_computation_count = sum(!is.na(get(value_column))),
      outlier_count = sum(is_outlier),
      threshold_boundary_count = sum(is_boundary_condition)
    ),
    by = group_columns
  ]

  return(list(outliers = outlier_dt[], diagnostics = outlier_diagnostics[]))
}

#' @title Compute temporal MAD-z anomalies from trend deviations
#' @param tendencies_dt Grouped tendencies table.
#' @param anomaly_config Resolved anomaly configuration.
#' @return Named list with anomalies and diagnostics tables.
compute_anomalies_table <- function(tendencies_dt, anomaly_config) {
  group_columns <- anomaly_config$group_by

  scored_dt <- data.table::copy(tendencies_dt)

  data.table::setorderv(
    scored_dt,
    c(group_columns, "..temporal_year", "..temporal_sub_key"),
    na.last = TRUE
  )

  scored_dt[, trend_residual := absolute_change]

  scored_dt[, `:=`(
    residual_count = sum(!is.na(trend_residual)),
    residual_median = stats::median(trend_residual, na.rm = TRUE),
    residual_mad = stats::mad(
      trend_residual,
      center = stats::median(trend_residual, na.rm = TRUE),
      constant = 1,
      na.rm = TRUE
    )
  ), by = group_columns]

  scored_dt[, robust_z_mad := data.table::fifelse(
    residual_mad > 0,
    (anomaly_config$mad_scale_constant * (trend_residual - residual_median)) /
      residual_mad,
    NA_real_
  )]

  scored_dt[, score_status := data.table::fifelse(
    residual_count < anomaly_config$min_group_size,
    "insufficient_group_size",
    data.table::fifelse(
      residual_mad == 0 | is.na(residual_mad),
      "non_scoreable_zero_dispersion",
      "successful_computation"
    )
  )]

  scored_dt[, is_boundary_condition := abs(abs(robust_z_mad) - anomaly_config$mad_z_threshold) <= anomaly_config$boundary_tolerance]

  scored_dt[, is_anomaly :=
    score_status == "successful_computation" &
      abs(robust_z_mad) >= anomaly_config$mad_z_threshold
  ]

  anomaly_dt <- scored_dt[is_anomaly == TRUE]
  data.table::setorderv(
    anomaly_dt,
    c(group_columns, "..temporal_year", "..temporal_sub_key"),
    na.last = TRUE
  )

  anomaly_diagnostics <- scored_dt[
    ,
    .(
      successful_computation_count = sum(score_status == "successful_computation"),
      non_scoreable_count = sum(score_status == "non_scoreable_zero_dispersion"),
      insufficient_group_size_count = sum(score_status == "insufficient_group_size"),
      threshold_boundary_count = sum(is_boundary_condition, na.rm = TRUE),
      anomaly_count = sum(is_anomaly)
    ),
    by = group_columns
  ]

  return(list(anomalies = anomaly_dt[], diagnostics = anomaly_diagnostics[]))
}

#' @title Build anomaly diagnostics table
#' @param tendency_diag Tendencies diagnostics table.
#' @param outlier_diag Outlier diagnostics table.
#' @param anomaly_diag Anomaly diagnostics table.
#' @param group_columns Group key columns.
#' @return Diagnostics data table.
build_diagnostics_table <- function(
  tendency_diag,
  outlier_diag,
  anomaly_diag,
  group_columns
) {
  diagnostics_dt <- merge(
    tendency_diag,
    outlier_diag,
    by = group_columns,
    all = TRUE,
    suffixes = c("_tendencies", "_outliers")
  )

  diagnostics_dt <- merge(
    diagnostics_dt,
    anomaly_diag,
    by = group_columns,
    all = TRUE,
    suffixes = c("", "_anomalies")
  )

  data.table::setorderv(diagnostics_dt, group_columns, na.last = TRUE)

  return(diagnostics_dt[])
}

#' @title Validate required columns
#' @param data_dt Data table.
#' @param required_columns Character vector of required columns.
#' @param context Character label for errors.
#' @return Invisible TRUE when valid.
validate_required_columns <- function(data_dt, required_columns, context) {
  checkmate::assert_data_table(data_dt)
  checkmate::assert_character(required_columns, min.len = 1, any.missing = FALSE)
  checkmate::assert_string(context, min.chars = 1)

  missing_columns <- setdiff(required_columns, names(data_dt))

  if (length(missing_columns) > 0L) {
    cli::cli_abort(c(
      "missing required columns for anomaly computation.",
      "x" = "context: {.val {context}}",
      "x" = "missing columns: {.val {missing_columns}}"
    ))
  }

  return(invisible(TRUE))
}
