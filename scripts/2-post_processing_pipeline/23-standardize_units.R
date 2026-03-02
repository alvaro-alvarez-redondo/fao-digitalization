# script: units standardization stage functions
# description: validate and apply numeric unit conversions and run numeric standarization.


#' @title Validate required rule-table columns
#' @description Validates presence and non-missingness of required columns.
#' @param rule_dt Data.frame/data.table containing rule rows.
#' @param required_columns Character vector of required column names.
#' @param rule_label Character scalar label used in error messages.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_data_frame assert_character assert_string
validate_rule_schema <- function(rule_dt, required_columns, rule_label) {
  checkmate::assert_data_frame(rule_dt, min.rows = 1)
  checkmate::assert_character(required_columns, min.len = 1, any.missing = FALSE)
  checkmate::assert_string(rule_label, min.chars = 1)

  missing_columns <- setdiff(required_columns, names(rule_dt))
  if (length(missing_columns) > 0) {
    cli::cli_abort(c(
      "Missing required columns in {.val {rule_label}} rules.",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  columns_with_na <- required_columns[vapply(required_columns, function(column_name) {
    anyNA(rule_dt[[column_name]])
  }, logical(1))]

  if (length(columns_with_na) > 0) {
    cli::cli_abort(c(
      "Found missing values in required {.val {rule_label}} rule columns.",
      "x" = paste(columns_with_na, collapse = ", ")
    ))
  }

  return(invisible(TRUE))
}

#' @title validate conversion rules
#' @description validate numeric conversion-rule schema, uniqueness, and
#' deterministic idempotency constraints.
#' @param conversion_dt conversion rules data.table/data.frame.
#' @return invisible true.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \\dontrun{validate_conversion_rules(conversion_dt)}
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
    "standarization conversion"
  )

  conversion_dt <- data.table::as.data.table(conversion_dt)

  duplicate_rows <- conversion_dt[
    ,
    .N,
    by = .(product, from_unit)
  ][N > 1]

  if (nrow(duplicate_rows) > 0) {
    cli::cli_abort(
      "conversion rules contain duplicate {.val (product, from_unit)} definitions"
    )
  }

  numeric_factor <- suppressWarnings(as.numeric(conversion_dt$factor))
  numeric_offset <- suppressWarnings(as.numeric(conversion_dt$offset))

  if (any(!is.finite(numeric_factor))) {
    cli::cli_abort("conversion factor values must be finite")
  }

  if (any(!is.finite(numeric_offset))) {
    cli::cli_abort("conversion offset values must be finite")
  }

  conversion_dt[, product_key := normalize_string(product)]
  conversion_dt[, from_unit_key := normalize_string(from_unit)]
  conversion_dt[, to_unit_key := normalize_string(to_unit)]

  chained_rules <- merge(
    conversion_dt[, .(product_key, from_unit_key)],
    conversion_dt[, .(product_key, to_unit_key)],
    by.x = c("product_key", "from_unit_key"),
    by.y = c("product_key", "to_unit_key")
  )

  if (nrow(chained_rules) > 0) {
    cli::cli_abort(
      "conversion rules create chained conversions for the same product; this can trigger double conversion on repeated runs"
    )
  }

  return(invisible(TRUE))
}


#' @title load units standardization rules
#' @description ensure number-standarization folder exists, create template if
#' missing, and load numeric conversion rules.
#' @param config named configuration list.
#' @return named list with `layer_rules` and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom here here
#' @examples
#' \\dontrun{load_units_standardization_rules(config)}
load_units_standardization_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$harmonization, min.chars = 1)

  harmonization_dir <- config$paths$data$imports$harmonization
  template_dir <- here::here("data", "3-export", "templates")
  fs::dir_create(harmonization_dir, recurse = TRUE)
  fs::dir_create(template_dir, recurse = TRUE)

  template_path <- fs::path(
    template_dir,
    "standardize_units_template.xlsx"
  )

  if (!file.exists(template_path)) {
    cli::cli_alert_info("Creating numeric standarization template...")

    numeric_template <- data.table::data.table(
      product = character(0),
      from_unit = character(0),
      to_unit = character(0),
      factor = numeric(0),
      offset = numeric(0)
    )

    workbook <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(workbook, "units_standardization")
    openxlsx::writeData(workbook, "units_standardization", numeric_template)
    openxlsx::saveWorkbook(workbook, template_path, overwrite = FALSE)
  }

  layer_payload <- list(
    layer_rules = read_rule_table(template_path),
    source_path = template_path
  )

  return(layer_payload)
}


#' @title apply units standardization mapping
#' @description apply numeric unit conversions using vectorized keyed joins.
#' @param mapped_dt data.table/data.frame to standarize.
#' @param conversion_dt numeric conversion rules data.table/data.frame.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return named list with `data`, `matched_count`, `unmatched_count`.
#' @importFrom checkmate assert_data_frame assert_string
#' @examples
#' \\dontrun{apply_units_standardization_mapping(mapped_dt, conversion_dt, "unit", "value", "product")}
apply_units_standardization_mapping <- function(
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

  validate_conversion_rules(conversion_dt)

  normalized_dt <- data.table::copy(data.table::as.data.table(mapped_dt))
  active_conversion <- data.table::as.data.table(conversion_dt)

  active_conversion[, product_key := normalize_string(product)]
  active_conversion[, unit_key := normalize_string(from_unit)]
  active_conversion[, factor_num := as.numeric(factor)]
  active_conversion[, offset_num := as.numeric(offset)]
  data.table::setkey(active_conversion, product_key, unit_key)

  product_keys <- normalize_string(normalized_dt[[product_column]])
  unit_keys <- normalize_string(normalized_dt[[unit_column]])

  join_input <- data.table::data.table(
    product_key = product_keys,
    unit_key = unit_keys
  )

  join_result <- active_conversion[join_input]
  is_matched <- !is.na(join_result$to_unit)

  numeric_values <- coerce_numeric_safe(normalized_dt[[value_column]])
  invalid_mask <- !is.na(normalized_dt[[value_column]]) & is.na(numeric_values)

  if (any(invalid_mask)) {
    invalid_values <- unique(as.character(normalized_dt[[value_column]][invalid_mask]))
    cli::cli_abort(
      "value column contains non-numeric values that cannot be standarized: {paste(invalid_values, collapse = ', ')}"
    )
  }

  if (any(is_matched)) {
    numeric_values[is_matched] <-
      numeric_values[is_matched] * join_result$factor_num[is_matched] +
      join_result$offset_num[is_matched]

    normalized_dt[is_matched, (unit_column) := join_result$to_unit[is_matched]]
  }

  normalized_dt[, (value_column) := numeric_values]

  unmatched_count <- sum(!is_matched & !is.na(unit_keys) & nzchar(unit_keys))

  return(list(
    data = normalized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}


#' @title run units standardization layer batch
#' @description orchestrate number standarization stage with rule loading,
#' vectorized conversion application, and diagnostics attachment.
#' @param cleaned_dt cleaned data.table/data.frame.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return standarized data.table with diagnostics attached.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @examples
#' \\dontrun{run_units_standardization_layer_batch(cleaned_dt, config)}
run_standardize_units_layer_batch <- function(
  cleaned_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product"
) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(product_column, min.chars = 1)

  normalized_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  layer_payload <- load_units_standardization_rules(config)
  layer_rules <- layer_payload$layer_rules

  if (nrow(layer_rules) > 0) {
    result <- apply_units_standardization_mapping(
      normalized_dt,
      layer_rules,
      unit_column,
      value_column,
      product_column
    )
    normalized_dt <- result$data

    diagnostics_audit_dt <- if (result$matched_count > 0L) {
      data.table::data.table(affected_rows = as.integer(result$matched_count))
    } else {
      data.table::data.table(affected_rows = integer(0))
    }

    diagnostics <- build_layer_diagnostics(
      layer_name = "standardize_units",
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(normalized_dt),
      audit_dt = diagnostics_audit_dt
    )
  } else {
    diagnostics <- build_layer_diagnostics(
      layer_name = "standardize_units",
      rows_in = nrow(cleaned_dt),
      rows_out = nrow(cleaned_dt),
      audit_dt = data.table::data.table(affected_rows = integer(0))
    )
    diagnostics$messages <- "no numeric standarization rules found"
  }

  attr(normalized_dt, "layer_diagnostics") <- list(
    standardize_units = diagnostics
  )

  return(normalized_dt)
}

# backward-compatible aliases
run_number_standarization_layer_batch <- run_standardize_units_layer_batch
run_number_harmonization_layer_batch <- run_standardize_units_layer_batch
load_standardize_units_rules <- load_units_standardization_rules
load_numeric_standarization_rules <- load_units_standardization_rules
load_numeric_harmonization_rules <- load_units_standardization_rules
apply_number_standarization_mapping <- apply_units_standardization_mapping
apply_number_harmonization_mapping <- apply_units_standardization_mapping
