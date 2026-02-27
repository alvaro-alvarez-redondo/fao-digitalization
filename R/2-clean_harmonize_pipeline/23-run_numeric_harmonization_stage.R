# script: numeric harmonization stage functions
# description: validate and apply numeric unit conversions and run numeric harmonization.


#' @title validate conversion rules
#' @description validate numeric conversion rule schema and active uniqueness.
#' @param conversion_dt conversion rules data.table.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \dontrun{validate_conversion_rules(conversion_dt)}
validate_conversion_rules <- function(conversion_dt) {
  checkmate::assert_data_frame(conversion_dt, min.rows = 1)

  required_columns <- c(
    "product",
    "from_unit",
    "to_unit",
    "factor",
    "offset"
  )

  validate_rule_schema(
    conversion_dt,
    required_columns,
    "harmonization conversion"
  )

  duplicate_rows <- conversion_dt[, .N, by = .(from_unit, to_unit)][N > 1]

  if (nrow(duplicate_rows) > 0) {
    cli::cli_abort("conversion rules contain duplicate unit pairs")
  }

  if (any(!is.finite(as.numeric(conversion_dt$factor)))) {
    cli::cli_abort("conversion factor values must be finite")
  }

  if (any(!is.finite(as.numeric(conversion_dt$offset)))) {
    cli::cli_abort("conversion offset values must be finite")
  }

  return(invisible(TRUE))
}


#' @title load number harmonization rules
#' @description ensure number harmonization folder exists, create template if missing,
#' and load numeric conversion rules.
#' @param config named configuration list.
#' @return named list with `conversion_rules`, `template_created`, and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create dir_ls path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
load_numeric_harmonization_rules <- function(config) {
  harmonization_dir <- config$paths$data$imports$harmonization
  fs::dir_create(harmonization_dir, recurse = TRUE)

  file_path <- fs::path(harmonization_dir, "numeric_harmonization.xlsx")

  if (!file.exists(file_path)) {
    cli::cli_alert_info("Creating numeric harmonization template...")
    numeric_template <- data.table::data.table(
      product = character(0),
      from_unit = character(0),
      to_unit = character(0),
      factor = numeric(0),
      offset = numeric(0)
    )
    wb <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(wb, "number_harmonization")
    openxlsx::writeData(wb, "number_harmonization", numeric_template)
    openxlsx::saveWorkbook(wb, file_path, overwrite = FALSE)
  }

  list(
    layer_rules = read_rule_table(file_path),
    source_path = file_path
  )
}


#' @title apply number harmonization mapping
#' @description apply numeric unit conversions to a dataset using conversion rules.
#' @param mapped_dt data.table to harmonize.
#' @param conversion_dt numeric conversion rules data.table.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return named list with `data`, `matched_count`, `unmatched_count`.
#' @importFrom checkmate assert_data_frame assert_string
apply_number_harmonization_mapping <- function(
  mapped_dt,
  conversion_dt,
  unit_column,
  value_column,
  product_column
) {
  checkmate::assert_data_frame(mapped_dt, min.rows = 0)
  checkmate::assert_data_frame(conversion_dt, min.rows = 1)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(product_column, min.chars = 1)

  if (!unit_column %in% colnames(mapped_dt)) {
    cli::cli_abort("unit column {.val {unit_column}} is missing")
  }
  if (!value_column %in% colnames(mapped_dt)) {
    cli::cli_abort("value column {.val {value_column}} is missing")
  }
  if (!product_column %in% colnames(mapped_dt)) {
    cli::cli_abort("product column {.val {product_column}} is missing")
  }

  harmonized_dt <- data.table::copy(data.table::as.data.table(mapped_dt))
  active_conversion <- data.table::as.data.table(conversion_dt)

  # Normalize for join
  active_conversion[, product_key := normalize_string(product)]
  active_conversion[, unit_key := normalize_string(from_unit)]
  data.table::setkey(active_conversion, product_key, unit_key)

  product_keys <- normalize_string(harmonized_dt[[product_column]])
  unit_keys <- normalize_string(harmonized_dt[[unit_column]])

  matched_index <- active_conversion[.(product_keys, unit_keys), which = TRUE]
  is_matched <- !is.na(matched_index)

  numeric_values <- coerce_numeric_safe(harmonized_dt[[value_column]])

  # Fail only if non-empty, non-numeric values exist
  invalid_values <- harmonized_dt[[value_column]][
    !is.na(harmonized_dt[[value_column]]) &
      is.na(numeric_values)
  ]

  if (length(invalid_values) > 0) {
    cli::cli_abort(
      "value column contains non-numeric values that cannot be harmonized: {paste(unique(invalid_values), collapse = ', ')}"
    )
  }

  # Apply conversions
  if (any(is_matched)) {
    matched_factors <- as.numeric(active_conversion$factor[matched_index[
      is_matched
    ]])
    matched_offsets <- as.numeric(active_conversion$offset[matched_index[
      is_matched
    ]])

    numeric_values[is_matched] <- numeric_values[is_matched] *
      matched_factors +
      matched_offsets
    harmonized_dt[
      is_matched,
      (unit_column) := active_conversion$to_unit[matched_index[is_matched]]
    ]
  }

  harmonized_dt[, (value_column) := numeric_values]
  unmatched_count <- length(unique(unit_keys[!is_matched & unit_keys != ""]))

  return(list(
    data = harmonized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}


#' @title run number harmonization layer
#' @description orchestrate number harmonization stage with rule loading, application,
#' diagnostics, and persistence. Creates template if missing.
#' @param cleaned_dt cleaned data.table.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return harmonized data.table with diagnostics attached.
#' @importFrom checkmate assert_data_frame assert_list assert_choice
run_number_harmonization_layer_batch <- function(
  cleaned_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product"
) {
  harmonized_dt <- data.table::copy(cleaned_dt)
  payload <- load_numeric_harmonization_rules(config)
  layer_rules <- payload$layer_rules

  if (nrow(layer_rules) > 0) {
    result <- apply_number_harmonization_mapping(
      harmonized_dt,
      layer_rules,
      unit_column,
      value_column,
      product_column
    )
    harmonized_dt <- result$data

    diagnostics <- build_layer_diagnostics(
      layer_name = "numeric_harmonization",
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(harmonized_dt),
      matched_count = result$matched_count,
      unmatched_count = result$unmatched_count
    )
  } else {
    diagnostics <- build_layer_diagnostics(
      layer_name = "numeric_harmonization",
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(cleaned_dt),
      matched_count = 0L,
      unmatched_count = 0L,
      status = "warn",
      messages = "no numeric harmonization rules found"
    )
  }

  attr(harmonized_dt, "layer_diagnostics") <- list(
    numeric_harmonization = diagnostics
  )
  return(harmonized_dt)
}


#' @title run cleaning, harmonization, and number harmonization pipeline
#' @description orchestrate all stages: cleaning, general harmonization, numeric harmonization.
#' Collects diagnostics for each stage and returns the final dataset.
#' @param raw_dt raw data.frame/data.table.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name for numeric harmonization (default: "unit").
#' @param value_column character scalar numeric value column name (default: "value").
#' @param product_column character scalar product column name (default: "product").
#' @return final data.table with `pipeline_diagnostics` and `pipeline_diagnostics_paths` attributes.
#' @importFrom checkmate assert_data_frame assert_list
run_clean_harmonize_pipeline_batch <- function(
  raw_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product"
) {
  # Cleaning stage
  cleaned_dt <- run_cleaning_layer_batch(raw_dt, config)

  # Harmonization stage
  harmonized_dt <- run_harmonization_layer_batch(cleaned_dt, config)

  # Numeric harmonization stage
  number_dt <- run_number_harmonization_layer_batch(
    harmonized_dt,
    config,
    unit_column,
    value_column,
    product_column
  )

  # Aggregate diagnostics
  pipeline_diagnostics <- list(
    cleaning = attr(cleaned_dt, "layer_diagnostics"),
    harmonization = attr(harmonized_dt, "layer_diagnostics"),
    number_harmonization = attr(number_dt, "layer_diagnostics")
  )

  attr(number_dt, "pipeline_diagnostics") <- pipeline_diagnostics
  return(number_dt)
}

