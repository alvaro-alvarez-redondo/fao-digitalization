# script: units standardization stage functions
# description: validate and apply numeric unit conversions and run numeric standardization.

#' @title Validate required rule-table columns
#' @description Validates presence and non-missingness of required columns.
#' @param rule_dt Data.frame/data.table containing rule rows.
#' @param required_columns Character vector of required column names.
#' @param rule_label Character scalar label used in error messages.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_data_frame assert_character assert_string
validate_rule_schema <- function(rule_dt, required_columns, rule_label) {
  checkmate::assert_data_frame(rule_dt, min.rows = 1)
  checkmate::assert_character(
    required_columns,
    min.len = 1,
    any.missing = FALSE
  )
  checkmate::assert_string(rule_label, min.chars = 1)

  missing_columns <- setdiff(required_columns, names(rule_dt))
  if (length(missing_columns) > 0L) {
    cli::cli_abort(c(
      "Missing required columns in {.val {rule_label}} rules.",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  columns_with_na <- required_columns[vapply(
    required_columns,
    function(column_name) {
      anyNA(rule_dt[[column_name]])
    },
    logical(1)
  )]

  if (length(columns_with_na) > 0L) {
    cli::cli_abort(c(
      "Found missing values in required {.val {rule_label}} rule columns.",
      "x" = paste(columns_with_na, collapse = ", ")
    ))
  }

  return(invisible(TRUE))
}

#' @title Normalize conversion rule columns
#' @description Renames legacy conversion rule columns to cohesive internal names
#' (`source_unit`, `target_unit`, `multiplier`, `addend`) while preserving
#' backward compatibility for input files using legacy headers.
#' @param conversion_dt conversion rules data.table/data.frame.
#' @return data.table with normalized internal column names.
#' @importFrom checkmate assert_data_frame
normalize_conversion_rule_columns <- function(conversion_dt) {
  checkmate::assert_data_frame(conversion_dt, min.rows = 0)

  normalized_conversion_dt <- data.table::copy(data.table::as.data.table(
    conversion_dt
  ))

  rename_mapping <- c(
    from_unit = "source_unit",
    to_unit = "target_unit",
    factor = "multiplier",
    offset = "addend"
  )

  legacy_names <- names(rename_mapping)
  available_legacy <- legacy_names[
    legacy_names %in% names(normalized_conversion_dt)
  ]

  if (length(available_legacy) > 0L) {
    target_names <- unname(rename_mapping[available_legacy])
    rename_mask <- !target_names %in% names(normalized_conversion_dt)

    if (any(rename_mask)) {
      data.table::setnames(
        normalized_conversion_dt,
        old = available_legacy[rename_mask],
        new = target_names[rename_mask]
      )
    }
  }

  return(normalized_conversion_dt)
}

#' @title Ensure standardize-units template exists
#' @description Creates `data/2-post_processing/templates` (via configured
#' audit root) when missing and initializes `standardize_units_template.xlsx`
#' with required columns when absent.
#' @param config Named configuration list.
#' @return Character scalar template file path.
#' @importFrom checkmate assert_list assert_string assert_directory_exists
#' @importFrom fs path
#' @importFrom writexl write_xlsx
#' @examples
#' \dontrun{ensure_standardize_template_exists(config)}
ensure_standardize_template_exists <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(
    config$paths$data$audit$audit_root_dir,
    min.chars = 1
  )

  templates_dir <- fs::path(config$paths$data$audit$audit_root_dir, "templates")

  ensure_directories_exist(templates_dir, recurse = TRUE)
  checkmate::assert_directory_exists(templates_dir)

  template_path <- fs::path(templates_dir, "standardize_units_template.xlsx")

  if (!file.exists(template_path)) {
    template_dt <- data.table::data.table(
      product = character(0),
      source_unit = character(0),
      target_unit = character(0),
      multiplier = numeric(0),
      addend = numeric(0)
    )

    writexl::write_xlsx(
      list(units_standardization = template_dt),
      path = template_path
    )
  }

  return(template_path)
}

#' @title Read all standardize rule Excel files
#' @description Discovers all Excel files in
#' `config$paths$data$imports$standardization` and reads each file
#' independently.
#' @param config named configuration list.
#' @return named list with `rules` and `source_paths`.
#' @importFrom checkmate assert_list assert_string assert_directory_exists
#'  dir_ls path_file
#' @examples
#' \dontrun{read_all_standardize_rule_files(config)}
read_all_standardize_rule_files <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(
    config$paths$data$imports$standardization,
    min.chars = 1
  )

  standardization_dir <- config$paths$data$imports$standardization
  ensure_directories_exist(standardization_dir, recurse = TRUE)
  checkmate::assert_directory_exists(standardization_dir)

  rule_paths <- fs::dir_ls(
    path = standardization_dir,
    regexp = "\\.(xlsx|xls)$",
    type = "file"
  ) |>
    sort()

  if (length(rule_paths) == 0L) {
    return(list(
      rules = data.table::data.table(),
      source_paths = character(0)
    ))
  }

  rules_by_file <- lapply(rule_paths, function(rule_path) {
    tryCatch(
      {
        file_rules_dt <- read_rule_table(rule_path)
        file_rules_dt <- normalize_conversion_rule_columns(file_rules_dt)
        file_rules_dt[, source_rule_file := fs::path_file(rule_path)]

        return(file_rules_dt)
      },
      error = function(error_condition) {
        cli::cli_abort(c(
          "invalid standardization rule file.",
          "x" = "file: {.file {rule_path}}",
          "x" = error_condition$message
        ))
      }
    )
  })

  combined_rules <- data.table::rbindlist(
    rules_by_file,
    use.names = TRUE,
    fill = TRUE
  )

  return(list(
    rules = combined_rules,
    source_paths = rule_paths
  ))
}

#' @title Validate conversion rules
#' @description Validates numeric conversion-rule schema, uniqueness, and
#' deterministic idempotency constraints.
#' @param conversion_dt conversion rules data.table/data.frame.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_data_frame
#' @examples
#' \dontrun{validate_conversion_rules(conversion_dt)}
validate_conversion_rules <- function(conversion_dt) {
  checkmate::assert_data_frame(conversion_dt, min.rows = 1)

  required_columns <- c(
    "product",
    "source_unit",
    "target_unit",
    "multiplier",
    "addend"
  )
  validate_rule_schema(
    conversion_dt,
    required_columns,
    "standardization conversion"
  )

  duplicate_rows <- conversion_dt[, .N, by = .(product, source_unit)][N > 1L]
  if (nrow(duplicate_rows) > 0L) {
    cli::cli_abort(
      "conversion rules contain duplicate {.val (product, source_unit)} definitions"
    )
  }

  multiplier_num <- suppressWarnings(as.numeric(conversion_dt$multiplier))
  addend_num <- suppressWarnings(as.numeric(conversion_dt$addend))

  if (any(!is.finite(multiplier_num))) {
    cli::cli_abort("conversion multiplier values must be finite")
  }

  if (any(!is.finite(addend_num))) {
    cli::cli_abort("conversion addend values must be finite")
  }

  source_pairs <- unique(data.table::data.table(
    product_key = normalize_string(conversion_dt$product),
    unit_key = normalize_string(conversion_dt$source_unit)
  ))
  target_pairs <- unique(data.table::data.table(
    product_key = normalize_string(conversion_dt$product),
    unit_key = normalize_string(conversion_dt$target_unit)
  ))

  data.table::setkey(source_pairs, product_key, unit_key)
  data.table::setkey(target_pairs, product_key, unit_key)

  chained_rules <- source_pairs[target_pairs, nomatch = 0L]

  if (nrow(chained_rules) > 0L) {
    cli::cli_abort(
      "conversion rules create chained conversions for the same product; this can trigger double conversion on repeated runs"
    )
  }

  return(invisible(TRUE))
}

#' @title Prepare standardization rules
#' @description Normalizes headers, validates merged rules, and materializes
#' numeric and key columns used for conversion joins.
#' @param raw_rules_dt conversion rules data.table/data.frame.
#' @return Prepared `data.table` suitable for conversion joins.
#' @importFrom checkmate assert_data_frame
prepare_standardize_rules <- function(raw_rules_dt) {
  checkmate::assert_data_frame(raw_rules_dt, min.rows = 0)

  prepared_rules_dt <- normalize_conversion_rule_columns(raw_rules_dt)

  if (nrow(prepared_rules_dt) == 0L) {
    return(prepared_rules_dt)
  }

  validate_conversion_rules(prepared_rules_dt)

  prepared_rules_dt[, multiplier_num := as.numeric(multiplier)]
  prepared_rules_dt[, addend_num := as.numeric(addend)]
  prepared_rules_dt[, product_key := normalize_string(product)]
  prepared_rules_dt[, unit_key := normalize_string(source_unit)]

  data.table::setkey(prepared_rules_dt, product_key, unit_key)

  return(prepared_rules_dt)
}

#' @title Apply prepared standardization rules
#' @description Applies prepared conversion rules to dataset values using keyed
#' vectorized joins.
#' @param mapped_dt data.table/data.frame to standardize.
#' @param prepared_rules_dt Prepared rule table from `prepare_standardize_rules()`.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @return named list with `data`, `matched_count`, and `unmatched_count`.
#' @importFrom checkmate assert_data_frame assert_string
apply_standardize_rules <- function(
  mapped_dt,
  prepared_rules_dt,
  unit_column,
  value_column,
  product_column
) {
  checkmate::assert_data_frame(mapped_dt, min.rows = 0)
  checkmate::assert_data_frame(prepared_rules_dt, min.rows = 0)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(product_column, min.chars = 1)

  if (data.table::is.data.table(mapped_dt)) {
    normalized_dt <- data.table::copy(mapped_dt)
  } else {
    normalized_dt <- data.table::as.data.table(mapped_dt)
  }

  if (!unit_column %in% names(normalized_dt)) {
    cli::cli_abort("unit column {.val {unit_column}} is missing")
  }
  if (!value_column %in% names(normalized_dt)) {
    cli::cli_abort("value column {.val {value_column}} is missing")
  }
  if (!product_column %in% names(normalized_dt)) {
    cli::cli_abort("product column {.val {product_column}} is missing")
  }

  numeric_values <- coerce_numeric_safe(normalized_dt[[value_column]])
  invalid_mask <- !is.na(normalized_dt[[value_column]]) & is.na(numeric_values)
  if (any(invalid_mask)) {
    invalid_values <- unique(as.character(normalized_dt[[value_column]][
      invalid_mask
    ]))
    cli::cli_abort(
      "value column contains non-numeric values that cannot be standardized: {paste(invalid_values, collapse = ', ')}"
    )
  }

  unit_keys <- normalize_string(normalized_dt[[unit_column]])

  if (nrow(prepared_rules_dt) == 0L) {
    normalized_dt[, (value_column) := numeric_values]

    return(list(
      data = normalized_dt,
      matched_count = 0L,
      unmatched_count = as.integer(sum(!is.na(unit_keys) & nzchar(unit_keys)))
    ))
  }

  product_keys <- normalize_string(normalized_dt[[product_column]])

  join_input <- data.table::data.table(
    product_key = product_keys,
    unit_key = unit_keys
  )
  join_result <- prepared_rules_dt[join_input]

  is_matched <- !is.na(join_result$target_unit)

  if (any(is_matched)) {
    numeric_values[is_matched] <-
      numeric_values[is_matched] *
      join_result$multiplier_num[is_matched] +
      join_result$addend_num[is_matched]

    normalized_dt[
      is_matched,
      (unit_column) := join_result$target_unit[is_matched]
    ]
  }

  normalized_dt[, (value_column) := numeric_values]

  unmatched_count <- sum(!is_matched & !is.na(unit_keys) & nzchar(unit_keys))

  return(list(
    data = normalized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title Aggregate standardized rows
#' @description Collapses rows where all columns except a numeric measure
#' (`value_column`) are identical by summing the measure. Preserves column
#' order and schema. Returns `NA` for groups where every value is `NA`;
#' otherwise sums non-`NA` values. Idempotent: re-running on an
#' already-unique table is a no-op.
#' @param dt `data.table` to aggregate.
#' @param value_column Character scalar name of the numeric column to sum.
#' @return Aggregated `data.table` with the same column order and schema.
#' @importFrom checkmate assert_data_table assert_string
#' @importFrom data.table setcolorder setnames setkeyv anyDuplicated
aggregate_standardized_rows <- function(dt, value_column = "value") {
  checkmate::assert_data_table(dt)
  checkmate::assert_string(value_column, min.chars = 1)

  if (!value_column %in% names(dt)) {
    cli::cli_abort("value column {.val {value_column}} not found in data")
  }

  if (nrow(dt) <= 1L) {
    return(dt)
  }

  group_cols <- setdiff(names(dt), value_column)

  if (length(group_cols) == 0L) {
    vals <- dt[[value_column]]
    agg_val <- if (all(is.na(vals))) NA_real_ else sum(vals, na.rm = TRUE)
    result <- data.table::data.table(agg_value_tmp_ = agg_val)
    data.table::setnames(result, "agg_value_tmp_", value_column)
    return(result)
  }

  if (anyDuplicated(dt, by = group_cols) == 0L) {
    return(dt)
  }

  original_order <- names(dt)
  has_na <- anyNA(dt[[value_column]])

  # Vectorized aggregation: sum(na.rm = TRUE) for the value column.
  # All-NA groups are corrected in a second pass below.
  result <- dt[,
    .(agg_value_tmp_ = sum(get(value_column), na.rm = TRUE)),
    by = group_cols
  ]
  data.table::setnames(result, "agg_value_tmp_", value_column)

  if (has_na) {
    na_counts <- dt[is.na(get(value_column)), .N, by = group_cols]

    if (nrow(na_counts) > 0L) {
      total_counts <- dt[, .N, by = group_cols]

      data.table::setnames(na_counts, "N", "na_count_tmp_")
      data.table::setnames(total_counts, "N", "total_count_tmp_")

      data.table::setkeyv(na_counts, group_cols)
      data.table::setkeyv(total_counts, group_cols)

      all_na_groups <- na_counts[total_counts, nomatch = 0L][
        na_count_tmp_ == total_count_tmp_
      ]

      if (nrow(all_na_groups) > 0L) {
        all_na_groups[, c("na_count_tmp_", "total_count_tmp_") := NULL]
        data.table::setkeyv(result, group_cols)
        data.table::setkeyv(all_na_groups, group_cols)
        result[all_na_groups, (value_column) := NA_real_]
      }
    }
  }

  data.table::setcolorder(result, original_order)
  return(result)
}

#' @title Attach standardize layer diagnostics
#' @description Creates and attaches standardized diagnostics payload to the
#' standardized dataset.
#' @param standardized_dt standardized data.table.
#' @param cleaned_rows_count Integer number of input rows.
#' @param matched_count Integer matched row count.
#' @param unmatched_count Integer unmatched row count.
#' @param rules_count Integer number of loaded rules.
#' @param rule_sources Character vector of source rule files.
#' @param aggregation_enabled Logical scalar whether aggregation was applied.
#' @param rows_before_aggregation Integer rows before aggregation (or `NULL`).
#' @param rows_after_aggregation Integer rows after aggregation (or `NULL`).
#' @return data.table with `layer_diagnostics` attribute.
#' @importFrom checkmate assert_data_frame assert_int assert_character
#'  assert_flag
attach_standardize_diagnostics <- function(
  standardized_dt,
  cleaned_rows_count,
  matched_count,
  unmatched_count,
  rules_count,
  rule_sources,
  aggregation_enabled = FALSE,
  rows_before_aggregation = NULL,
  rows_after_aggregation = NULL
) {
  checkmate::assert_data_frame(standardized_dt, min.rows = 0)
  checkmate::assert_int(cleaned_rows_count, lower = 0)
  checkmate::assert_int(matched_count, lower = 0)
  checkmate::assert_int(unmatched_count, lower = 0)
  checkmate::assert_int(rules_count, lower = 0)
  checkmate::assert_character(rule_sources, any.missing = FALSE)
  checkmate::assert_flag(aggregation_enabled)

  diagnostics_audit_dt <- if (matched_count > 0L) {
    data.table::data.table(affected_rows = as.integer(matched_count))
  } else {
    data.table::data.table(affected_rows = integer(0))
  }

  diagnostics <- build_layer_diagnostics(
    layer_name = "standardize_units",
    rows_in = cleaned_rows_count,
    rows_out = nrow(standardized_dt),
    audit_dt = diagnostics_audit_dt
  )

  diagnostics$unmatched_count <- as.integer(unmatched_count)
  diagnostics$applied_rules <- as.integer(rules_count)
  diagnostics$rule_sources <- unique(rule_sources)

  diagnostics$aggregation_enabled <- aggregation_enabled
  if (aggregation_enabled && !is.null(rows_before_aggregation)) {
    diagnostics$rows_before_aggregation <- as.integer(rows_before_aggregation)
    diagnostics$rows_after_aggregation <- as.integer(rows_after_aggregation)
    diagnostics$collapsed_rows_count <- as.integer(
      rows_before_aggregation - rows_after_aggregation
    )
    diagnostics$aggregated_groups_count <- as.integer(rows_after_aggregation)
  }

  if (rules_count == 0L) {
    diagnostics$messages <- "no numeric standardization rules found; row aggregation skipped"
    diagnostics$potential_warnings <- diagnostics$messages
  } else {
    diagnostics$potential_warnings <- character(0)
  }

  attr(standardized_dt, "layer_diagnostics") <- list(
    standardize_units = diagnostics
  )

  return(standardized_dt)
}

#' @title load units standardization rules
#' @description Ensures template availability and loads prepared conversion rules
#' from standardization import files.
#' @param config named configuration list.
#' @return named list with `layer_rules`, `source_path`, and `template_path`.
#' @importFrom checkmate assert_list assert_string
#' @examples
#' \dontrun{load_units_standardization_rules(config)}
load_units_standardization_rules <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(
    config$paths$data$imports$standardization,
    min.chars = 1
  )

  template_path <- ensure_standardize_template_exists(config)
  raw_rules_payload <- read_all_standardize_rule_files(config)
  prepared_rules <- prepare_standardize_rules(raw_rules_payload$rules)

  source_paths <- if (length(raw_rules_payload$source_paths) > 0L) {
    raw_rules_payload$source_paths
  } else {
    template_path
  }

  return(list(
    layer_rules = prepared_rules,
    source_path = source_paths,
    template_path = template_path
  ))
}

#' @title run units standardization layer batch
#' @description Orchestrates standardization rule loading, conversion execution,
#' optional post-standardization row aggregation, and diagnostics attachment.
#' @param cleaned_dt cleaned data.table/data.frame.
#' @param config named configuration list.
#' @param unit_column character scalar unit column name.
#' @param value_column character scalar numeric value column name.
#' @param product_column character scalar product column name.
#' @param aggregate_after_standardize Logical scalar toggle for post-
#' standardization row aggregation. When `TRUE` (default), rows that are
#' identical on every column except `value_column` are collapsed by summing
#' the numeric measure. Aggregation is only applied when at least one
#' standardization rule is loaded; if no rules are present the data is
#' returned unchanged regardless of this flag.
#' @return standardized data.table with diagnostics attached.
#' @importFrom checkmate assert_data_frame assert_list assert_string
#'  assert_flag
#' @examples
#' \dontrun{run_units_standardization_layer_batch(cleaned_dt, config)}
run_standardize_units_layer_batch <- function(
  cleaned_dt,
  config,
  unit_column = "unit",
  value_column = "value",
  product_column = "product",
  aggregate_after_standardize = TRUE
) {
  checkmate::assert_data_frame(cleaned_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(unit_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(product_column, min.chars = 1)
  checkmate::assert_flag(aggregate_after_standardize)

  layer_payload <- load_units_standardization_rules(config)

  apply_result <- apply_standardize_rules(
    mapped_dt = cleaned_dt,
    prepared_rules_dt = layer_payload$layer_rules,
    unit_column = unit_column,
    value_column = value_column,
    product_column = product_column
  )

  rows_before_aggregation <- nrow(apply_result$data)
  has_rules <- nrow(layer_payload$layer_rules) > 0L
  if (aggregate_after_standardize && rows_before_aggregation > 0L && has_rules) {
    apply_result$data <- aggregate_standardized_rows(
      data.table::as.data.table(apply_result$data),
      value_column = value_column
    )
  }
  rows_after_aggregation <- nrow(apply_result$data)

  normalized_dt <- attach_standardize_diagnostics(
    standardized_dt = apply_result$data,
    cleaned_rows_count = nrow(cleaned_dt),
    matched_count = as.integer(apply_result$matched_count),
    unmatched_count = as.integer(apply_result$unmatched_count),
    rules_count = as.integer(nrow(layer_payload$layer_rules)),
    rule_sources = as.character(layer_payload$source_path),
    aggregation_enabled = aggregate_after_standardize,
    rows_before_aggregation = as.integer(rows_before_aggregation),
    rows_after_aggregation = as.integer(rows_after_aggregation)
  )

  return(normalized_dt)
}
