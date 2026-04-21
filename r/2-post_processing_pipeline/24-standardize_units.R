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
#' (`product_key`, `unit_source`, `unit_target`, `unit_multiplier`,
#' `unit_offset`) while preserving
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
    product = "product_key",
    source_unit = "unit_source",
    target_unit = "unit_target",
    multiplier = "unit_multiplier",
    addend = "unit_offset",
    from_unit = "unit_source",
    to_unit = "unit_target",
    factor = "unit_multiplier",
    offset = "unit_offset"
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
      product_key = character(0),
      unit_source = character(0),
      unit_target = character(0),
      unit_multiplier = numeric(0),
      unit_offset = numeric(0)
    )

    writexl::write_xlsx(
      list(units_standardization = template_dt),
      path = template_path
    )
  }

  return(template_path)
}

#' @title Read one standardization workbook
#' @description Reads all worksheet tabs in a standardization workbook except
#' explicitly excluded sheet names (for example `master_unit`), normalizes
#' column names, and keeps only sheets that contain required standardization
#' rule columns.
#' @param rule_path Character scalar path to one workbook.
#' @param excluded_sheet_names Character vector of sheet names to skip.
#' @return `data.table` with standardized rule rows.
#' @importFrom checkmate assert_string assert_character assert_file_exists
#' @importFrom readxl excel_sheets read_excel
read_standardize_rule_workbook <- function(
  rule_path,
  excluded_sheet_names = character(0)
) {
  checkmate::assert_string(rule_path, min.chars = 1)
  checkmate::assert_file_exists(rule_path)
  checkmate::assert_character(excluded_sheet_names, any.missing = FALSE)

  required_columns <- c(
    "product_key",
    "unit_source",
    "unit_target",
    "unit_multiplier",
    "unit_offset"
  )

  workbook_sheet_names <- readxl::excel_sheets(rule_path)
  normalized_excluded <- normalize_string(excluded_sheet_names)

  selected_sheet_names <- workbook_sheet_names[
    !(normalize_string(workbook_sheet_names) %in% normalized_excluded)
  ]

  if (length(selected_sheet_names) == 0L) {
    cli::cli_abort(c(
      "No worksheets available for standardization after exclusions.",
      "x" = "file: {.file {rule_path}}",
      "x" = paste0("excluded sheets: ", paste(excluded_sheet_names, collapse = ", ")),
      "x" = paste0("available sheets: ", paste(workbook_sheet_names, collapse = ", "))
    ))
  }

  matched_sheet_tables <- lapply(selected_sheet_names, function(sheet_name) {
    sheet_dt <- readxl::read_excel(rule_path, sheet = sheet_name) |>
      data.table::as.data.table()

    sheet_dt <- normalize_conversion_rule_columns(sheet_dt)

    if (!all(required_columns %in% colnames(sheet_dt))) {
      return(NULL)
    }

    return(sheet_dt[, ..required_columns])
  })

  matched_sheet_indexes <- which(vapply(
    matched_sheet_tables,
    function(sheet_dt) {
      !is.null(sheet_dt)
    },
    logical(1)
  ))

  if (length(matched_sheet_indexes) == 0L) {
    cli::cli_abort(c(
      "No worksheets with matching standardization columns found.",
      "x" = "file: {.file {rule_path}}",
      "x" = paste0("required columns: ", paste(required_columns, collapse = ", ")),
      "x" = paste0("selected sheets: ", paste(selected_sheet_names, collapse = ", "))
    ))
  }

  matched_sheet_names <- selected_sheet_names[matched_sheet_indexes]
  matched_sheet_list <- matched_sheet_tables[matched_sheet_indexes]
  names(matched_sheet_list) <- matched_sheet_names

  return(data.table::rbindlist(
    matched_sheet_list,
    use.names = TRUE,
    fill = TRUE,
    idcol = "source_rule_sheet"
  ))
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

  excluded_sheet_names <-
    get_pipeline_constants()$post_processing$standardization$excluded_sheet_names
  checkmate::assert_character(excluded_sheet_names, any.missing = FALSE)

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
        file_rules_dt <- read_standardize_rule_workbook(
          rule_path = rule_path,
          excluded_sheet_names = excluded_sheet_names
        )
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
    "product_key",
    "unit_source",
    "unit_target",
    "unit_multiplier",
    "unit_offset"
  )
  validate_rule_schema(
    conversion_dt,
    required_columns,
    "standardization conversion"
  )

  duplicate_rows <- conversion_dt[, .N, by = .(product_key, unit_source)][N > 1L]
  if (nrow(duplicate_rows) > 0L) {
    cli::cli_abort(
      "conversion rules contain duplicate {.val (product_key, unit_source)} definitions"
    )
  }

  unit_multiplier_num <- suppressWarnings(as.numeric(conversion_dt$unit_multiplier))
  unit_offset_num <- suppressWarnings(as.numeric(conversion_dt$unit_offset))

  if (any(!is.finite(unit_multiplier_num))) {
    cli::cli_abort("conversion unit_multiplier values must be finite")
  }

  if (any(!is.finite(unit_offset_num))) {
    cli::cli_abort("conversion unit_offset values must be finite")
  }

  source_pairs <- unique(data.table::data.table(
    product_match_key = normalize_string(conversion_dt$product_key),
    unit_match_key = normalize_string(conversion_dt$unit_source)
  ))
  target_pairs <- unique(data.table::data.table(
    product_match_key = normalize_string(conversion_dt$product_key),
    unit_match_key = normalize_string(conversion_dt$unit_target)
  ))

  data.table::setkey(source_pairs, product_match_key, unit_match_key)
  data.table::setkey(target_pairs, product_match_key, unit_match_key)

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

  prepared_rules_dt[, unit_multiplier_num := as.numeric(unit_multiplier)]
  prepared_rules_dt[, unit_offset_num := as.numeric(unit_offset)]
  prepared_rules_dt[, product_match_key := normalize_string(product_key)]
  prepared_rules_dt[, unit_source_key := normalize_string(unit_source)]

  data.table::setkey(prepared_rules_dt, product_match_key, unit_source_key)

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

  raw_value_input <- normalized_dt[[value_column]]
  blank_string_mask <- rep(FALSE, length(raw_value_input))

  if (is.character(raw_value_input)) {
    blank_string_mask <- !is.na(raw_value_input) & trimws(raw_value_input) == ""
  }

  invalid_mask <-
    !is.na(raw_value_input) &
    !blank_string_mask &
    is.na(numeric_values)

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
    product_match_key = product_keys,
    unit_source_key = unit_keys
  )
  join_result <- prepared_rules_dt[
    join_input,
    .(unit_target, unit_multiplier_num, unit_offset_num)
  ]

  is_matched <- !is.na(join_result$unit_target)

  if (any(is_matched)) {
    matched_index <- which(is_matched)

    numeric_values[matched_index] <-
      numeric_values[matched_index] *
      join_result$unit_multiplier_num[matched_index] +
      join_result$unit_offset_num[matched_index]

    data.table::set(
      normalized_dt,
      i = matched_index,
      j = unit_column,
      value = join_result$unit_target[matched_index]
    )
  }

  normalized_dt[, (value_column) := numeric_values]

  unmatched_count <- sum(!is_matched & !is.na(unit_keys) & nzchar(unit_keys))

  return(list(
    data = normalized_dt,
    matched_count = as.integer(sum(is_matched)),
    unmatched_count = as.integer(unmatched_count)
  ))
}

#' @title Compute duplicate-group row mask
#' @description Returns logical mask selecting rows that belong to duplicate
#' groups defined by `group_cols`.
#' @param dt `data.table` to inspect.
#' @param group_cols Character vector of grouping columns.
#' @return Logical vector with one element per row in `dt`.
#' @importFrom checkmate assert_data_table assert_character
compute_duplicate_group_row_mask <- function(dt, group_cols) {
  checkmate::assert_data_table(dt)
  checkmate::assert_character(group_cols, any.missing = FALSE)

  if (nrow(dt) == 0L || length(group_cols) == 0L) {
    return(rep(FALSE, nrow(dt)))
  }

  duplicate_forward <- duplicated(dt, by = group_cols)

  if (!any(duplicate_forward)) {
    return(rep(FALSE, nrow(dt)))
  }

  duplicate_backward <- duplicated(dt, by = group_cols, fromLast = TRUE)

  return(duplicate_forward | duplicate_backward)
}

#' @title Aggregate duplicate groups
#' @description Aggregates duplicate-group rows by summing `value_column` with
#' deterministic all-NA semantics.
#' @param dt `data.table` with rows to aggregate.
#' @param group_cols Character vector of grouping columns.
#' @param value_column Character scalar value column name.
#' @return Aggregated `data.table`.
#' @importFrom checkmate assert_data_table assert_character assert_string
aggregate_duplicate_groups <- function(dt, group_cols, value_column) {
  checkmate::assert_data_table(dt)
  checkmate::assert_character(group_cols, any.missing = FALSE)
  checkmate::assert_string(value_column, min.chars = 1)

  if (nrow(dt) == 0L) {
    return(dt[0L, ])
  }

  value_vector <- dt[[value_column]]

  if (!anyNA(value_vector)) {
    if (identical(value_column, "value")) {
      return(dt[, .(value = sum(value)), by = group_cols])
    }

    aggregated_dt <- dt[, .(agg_value_tmp_ = sum(get(value_column))), by = group_cols]
    data.table::setnames(aggregated_dt, "agg_value_tmp_", value_column)

    return(aggregated_dt)
  }

  if (identical(value_column, "value")) {
    aggregated_dt <- dt[, .(
      agg_value_tmp_ = sum(value, na.rm = TRUE),
      non_na_count_tmp_ = sum(!is.na(value))
    ), by = group_cols]
  } else {
    aggregated_dt <- dt[, {
      values <- get(value_column)

      .(
        agg_value_tmp_ = sum(values, na.rm = TRUE),
        non_na_count_tmp_ = sum(!is.na(values))
      )
    }, by = group_cols]
  }

  aggregated_dt[non_na_count_tmp_ == 0L, agg_value_tmp_ := NA_real_]
  aggregated_dt[, non_na_count_tmp_ := NULL]
  data.table::setnames(aggregated_dt, "agg_value_tmp_", value_column)

  return(aggregated_dt)
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
#' @importFrom data.table setcolorder setnames anyDuplicated
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

  duplicate_group_mask <- compute_duplicate_group_row_mask(
    dt = dt,
    group_cols = group_cols
  )

  if (!any(duplicate_group_mask)) {
    return(dt)
  }

  original_order <- names(dt)
  if (all(duplicate_group_mask)) {
    result <- aggregate_duplicate_groups(
      dt = dt,
      group_cols = group_cols,
      value_column = value_column
    )

    data.table::setcolorder(result, original_order)

    return(result)
  }

  unique_rows <- dt[!duplicate_group_mask]
  duplicate_rows <- dt[duplicate_group_mask]

  aggregated_duplicate_rows <- aggregate_duplicate_groups(
    dt = duplicate_rows,
    group_cols = group_cols,
    value_column = value_column
  )

  result <- data.table::rbindlist(
    list(unique_rows, aggregated_duplicate_rows),
    use.names = TRUE,
    fill = TRUE
  )

  data.table::setcolorder(result, original_order)

  return(result)
}

#' @title Extract rows that will be aggregated
#' @description Returns only the rows from a pre-aggregation data.table that
#' belong to duplicate groups — i.e. the rows that `aggregate_standardized_rows()`
#' will collapse by summing. Groups are defined by all columns except
#' `value_column`. If there are no duplicates, returns an empty data.table with
#' the same schema.
#' @param dt `data.table` before aggregation.
#' @param value_column Character scalar name of the numeric column to sum.
#' @return `data.table` containing only rows from duplicate groups, with the
#'   same column order and schema as `dt`.
#' @importFrom checkmate assert_data_table assert_string
#' @importFrom data.table anyDuplicated
extract_aggregated_rows <- function(dt, value_column = "value") {
  checkmate::assert_data_table(dt)
  checkmate::assert_string(value_column, min.chars = 1)

  if (!value_column %in% names(dt)) {
    cli::cli_abort("value column {.val {value_column}} not found in data")
  }

  if (nrow(dt) == 0L) {
    return(dt)
  }

  group_cols <- setdiff(names(dt), value_column)

  if (length(group_cols) == 0L) {
    return(dt[0L, ])
  }

  duplicate_group_mask <- compute_duplicate_group_row_mask(
    dt = dt,
    group_cols = group_cols
  )

  if (!any(duplicate_group_mask)) {
    return(dt[0L, ])
  }

  return(dt[duplicate_group_mask])
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
    diagnostics$messages <- "no numeric standardization rules found"
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
#' the numeric measure.
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
  aggregated_source_rows_dt <- data.table::as.data.table(apply_result$data)[0L, ]
  if (aggregate_after_standardize && rows_before_aggregation > 0L) {
    pre_agg_dt <- data.table::as.data.table(apply_result$data)
    aggregated_source_rows_dt <- extract_aggregated_rows(
      pre_agg_dt,
      value_column = value_column
    )
    apply_result$data <- aggregate_standardized_rows(
      pre_agg_dt,
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

  attr(normalized_dt, "aggregated_source_rows") <- aggregated_source_rows_dt

  return(normalized_dt)
}
