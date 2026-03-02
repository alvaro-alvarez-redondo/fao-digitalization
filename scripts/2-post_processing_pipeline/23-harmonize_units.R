# script: numeric harmonization stage functions
# description: validate and apply numeric unit conversions and run numeric harmonization.

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
    "harmonization conversion"
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


#' @title load number harmonization rules
#' @description ensure number-harmonization folder exists, create template if
#' missing, and load numeric conversion rules.
#' @param config named configuration list.
#' @return named list with `layer_rules` and `source_path`.
#' @importFrom checkmate assert_list assert_string
#' @importFrom fs dir_create path
#' @importFrom openxlsx createWorkbook addWorksheet writeData saveWorkbook
#' @importFrom here here
#' @examples
#' \\dontrun{load_numeric_harmonization_rules(config)}
load_numeric_harmonization_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$imports$harmonization, min.chars = 1)

  harmonization_dir <- config$paths$data$imports$harmonization
  template_dir <- here::here("data", "exports", "templates")
  fs::dir_create(harmonization_dir, recurse = TRUE)
  fs::dir_create(template_dir, recurse = TRUE)

  template_path <- fs::path(
    template_dir,
    "numeric_harmonization_template.xlsx"
  )

  if (!file.exists(template_path)) {
    cli::cli_alert_info("Creating numeric harmonization template...")

    numeric_template <- data.table::data.table(
      product = character(0),
      from_unit = character(0),
      to_unit = character(0),
      factor = numeric(0),
      offset = numeric(0)
    )

    workbook <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(workbook, "number_harmonization")
    openxlsx::writeData(workbook, "number_harmonization", numeric_template)
    openxlsx::saveWorkbook(workbook, template_path, overwrite = FALSE)
  }

  layer_payload <- list(
    layer_rules = read_rule_table(template_path),
    source_path = template_path
  )

  return(layer_payload)
}


#' @title apply number harmonization mapping
#' @description apply numeric unit conversions using vectorized keyed joins.
#' @param mapped_dt data.table/data.frame to harmonize.
#' @param conversion_dt numeric conversion rules data.table/data.frame.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return named list with `data`, `matched_count`, `unmatched_count`.
#' @importFrom checkmate assert_data_frame assert_string
#' @examples
#' \\dontrun{apply_number_harmonization_mapping(mapped_dt, conversion_dt, "unit", "value", "product")}
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

  validate_conversion_rules(conversion_dt)

  harmonized_dt <- data.table::copy(data.table::as.data.table(mapped_dt))
  active_conversion <- data.table::as.data.table(conversion_dt)

  active_conversion[, product_key := normalize_string(product)]
  active_conversion[, unit_key := normalize_string(from_unit)]
  active_conversion[, factor_num := as.numeric(factor)]
  active_conversion[, offset_num := as.numeric(offset)]
  data.table::setkey(active_conversion, product_key, unit_key)

  product_keys <- normalize_string(harmonized_dt[[product_column]])
  unit_keys <- normalize_string(harmonized_dt[[unit_column]])

  join_input <- data.table::data.table(
    product_key = product_keys,
    unit_key = unit_keys
  )

  join_result <- active_conversion[join_input]
  is_matched <- !is.na(join_result$to_unit)

  numeric_values <- coerce_numeric_safe(harmonized_dt[[value_column]])
  invalid_mask <- !is.na(harmonized_dt[[value_column]]) & is.na(numeric_values)

  if (any(invalid_mask)) {
    invalid_values <- unique(as.character(harmonized_dt[[value_column]][invalid_mask]))
    cli::cli_abort(
      "value column contains non-numeric values that cannot be harmonized: {paste(invalid_values, collapse = ', ')}"
    )
  }

  if (any(is_matched)) {
    numeric_values[is_matched] <-
      numeric_values[is_matched] * join_result$factor_num[is_matched] +
      join_result$offset_num[is_matched]

    harmonized_dt[is_matched, (unit_column) := join_result$to_unit[is_matched]]
  }

  harmonized_dt[, (value_column) := numeric_values]

  unmatched_count <- sum(!is_matched & !is.na(unit_keys) & nzchar(unit_keys))

  return(list(
    data = harmonized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}


#' @title run number harmonization layer batch
#' @description orchestrate number harmonization stage with rule loading,
#' vectorized conversion application, and diagnostics attachment.
#' @param cleaned_dt cleaned data.table/data.frame.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return harmonized data.table with diagnostics attached.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#' @examples
#' \\dontrun{run_number_harmonization_layer_batch(cleaned_dt, config)}
run_number_harmonization_layer_batch <- function(
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

  harmonized_dt <- data.table::copy(data.table::as.data.table(cleaned_dt))
  layer_payload <- load_numeric_harmonization_rules(config)
  layer_rules <- layer_payload$layer_rules

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
