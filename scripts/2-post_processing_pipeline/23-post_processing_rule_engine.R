# script: post-processing rule engine
# description: schema coercion, canonical rule validation, dictionary
# construction, vectorized matching/mutation engine, and rule payload
# application for the clean and harmonize post-processing stages.

#' @title Coerce rule schema to canonical columns
#' @description Enforces strict stage-specific canonical schema.
#' @param rule_dt Rule table as data.frame/data.table.
#' @param stage_name Character scalar execution stage label.
#' @param rule_file_id Character scalar rule file identifier.
#' @return Canonicalized `data.table` rule table.
#' @importFrom checkmate assert_data_frame assert_string
coerce_rule_schema <- function(rule_dt, stage_name, rule_file_id) {
  checkmate::assert_data_frame(rule_dt, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(rule_file_id, min.chars = 1)

  canonical_columns <- get_canonical_rule_columns(validated_stage_name)
  stage_prefix <- paste0("^", validated_stage_name, "_")

  canonical_dt <- data.table::as.data.table(rule_dt)
  available_columns <- colnames(canonical_dt)

  normalized_columns <- sub(stage_prefix, "", available_columns)
  duplicated_normalized_columns <- normalized_columns[duplicated(normalized_columns)]

  if (length(duplicated_normalized_columns) > 0L) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains duplicate columns after stage-prefix normalization.",
      "x" = paste(unique(duplicated_normalized_columns), collapse = ", ")
    ))
  }

  data.table::setnames(canonical_dt, available_columns, normalized_columns)
  available_columns <- colnames(canonical_dt)

  source_result_column <- get_stage_source_value_column(validated_stage_name)
  optional_columns <- source_result_column

  missing_columns <- setdiff(canonical_columns, available_columns)
  missing_required_columns <- setdiff(missing_columns, optional_columns)

  if (length(missing_required_columns) > 0L) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} is missing required columns.",
      "x" = paste(missing_required_columns, collapse = ", ")
    ))
  }

  unexpected_columns <- setdiff(available_columns, canonical_columns)
  if (length(unexpected_columns) > 0L) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains unexpected columns.",
      "x" = paste(unexpected_columns, collapse = ", ")
    ))
  }

  if (!(source_result_column %in% colnames(canonical_dt))) {
    canonical_dt[, (source_result_column) := NA_character_]
  }

  canonical_dt <- canonical_dt[, ..canonical_columns]

  return(canonical_dt)
}

#' @title Normalize permitted missing rule values for internal validation
#' @description Converts allowed missing values in conditional rule value fields
#' to an internal placeholder used only during validation joins/grouping, while
#' preserving original rule semantics for downstream application.
#' @param rules_dt Canonical rule table.
#' @param stage_name Character scalar stage label.
#' @param na_placeholder Character scalar internal placeholder token.
#' @return Named list with `rules_for_validation` and `allowed_na_columns`.
#' @importFrom checkmate assert_data_frame assert_string
normalize_rule_values_for_validation <- function(
  rules_dt,
  stage_name,
  na_placeholder = get_pipeline_constants()$na_placeholder
) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  allowed_na_columns <- intersect(
    c(
      "value_source_raw",
      "value_source_clean",
      "value_source_harmonize",
      "value_target_raw",
      "value_target_clean",
      "value_target_harmonize"
    ),
    colnames(rules_dt)
  )

  rules_for_validation <- data.table::copy(data.table::as.data.table(rules_dt))

  if (length(allowed_na_columns) > 0L) {
    rules_for_validation[
      ,
      (allowed_na_columns) := lapply(.SD, function(column_values) {
        replacement_values <- column_values

        if (is.character(replacement_values)) {
          replacement_values[trimws(replacement_values) == ""] <- na_placeholder
          replacement_values[is.na(replacement_values)] <- na_placeholder
        }

        return(replacement_values)
      }),
      .SDcols = allowed_na_columns
    ]
  }

  return(list(
    rules_for_validation = rules_for_validation,
    allowed_na_columns = allowed_na_columns
  ))
}

#' @title Ensure rule-referenced dataset columns exist
#' @description Adds any missing `column_source` or `column_target` columns
#' referenced by canonical rules to the dataset as `NA_character_` before
#' validation and rule execution.
#' @param dataset_dt Data table to mutate by reference.
#' @param rules_dt Canonical rule table.
#' @return Mutated `dataset_dt` with missing referenced columns initialized.
#' @importFrom checkmate assert_data_table assert_data_frame
#' @importFrom cli cli_abort
ensure_rule_referenced_columns <- function(dataset_dt, rules_dt) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(rules_dt, min.rows = 0)

  existing_columns <- colnames(dataset_dt)

  if (anyDuplicated(existing_columns) > 0L) {
    duplicated_columns <- unique(existing_columns[duplicated(existing_columns)])

    cli::cli_abort(c(
      "dataset contains duplicate column names before rule-column materialization.",
      "x" = paste(duplicated_columns, collapse = ", ")
    ))
  }

  if (nrow(rules_dt) == 0L) {
    return(dataset_dt)
  }

  referenced_columns <- unique(c(rules_dt$column_source, rules_dt$column_target))
  referenced_columns <- as.character(referenced_columns)
  referenced_columns <- trimws(referenced_columns)
  referenced_columns <- referenced_columns[!is.na(referenced_columns) & nzchar(referenced_columns)]

  if (length(referenced_columns) == 0L) {
    return(dataset_dt)
  }

  missing_columns <- referenced_columns[!(referenced_columns %in% existing_columns)]

  if (length(missing_columns) > 0L) {
    dataset_dt[, (missing_columns) := NA_character_]
  }

  return(dataset_dt)
}

#' @title Check type compatibility between rule values and dataset column
#' @description Validates that rule values can be safely cast to the type of
#' the corresponding dataset column (numeric, integer, or Date).
#' @param dataset_vector Atomic vector from the dataset column.
#' @param rule_values Atomic vector of rule values to check.
#' @param field_name Character scalar field label for error messages.
#' @param rule_file_id Character scalar rule file identifier for error context.
#' @param column_name Character scalar column name for error messages.
#' @return Invisibly returns `TRUE`.
#' @importFrom cli cli_abort
check_type_compatibility <- function(
  dataset_vector,
  rule_values,
  field_name,
  rule_file_id,
  column_name = "unknown"
) {
  non_missing_values <- rule_values[!is.na(rule_values)]

  if (is.factor(dataset_vector)) {
    non_missing_values <- as.character(non_missing_values)
  }

  if (is.numeric(dataset_vector)) {
    suppressWarnings(parsed_values <- as.numeric(non_missing_values))
    if (anyNA(parsed_values) && length(non_missing_values) > 0) {
      cli::cli_abort(c(
        "Type compatibility validation failed for {.file {rule_file_id}}.",
        "x" = paste0(
          field_name,
          " cannot be safely cast to numeric for column ",
          column_name
        )
      ))
    }
  }

  if (is.integer(dataset_vector)) {
    suppressWarnings(parsed_values <- as.integer(non_missing_values))
    if (anyNA(parsed_values) && length(non_missing_values) > 0) {
      cli::cli_abort(c(
        "Type compatibility validation failed for {.file {rule_file_id}}.",
        "x" = paste0(
          field_name,
          " cannot be safely cast to integer for column ",
          column_name
        )
      ))
    }
  }

  if (inherits(dataset_vector, "Date")) {
    suppressWarnings(parsed_values <- as.Date(non_missing_values))
    if (anyNA(parsed_values) && length(non_missing_values) > 0) {
      cli::cli_abort(c(
        "Type compatibility validation failed for {.file {rule_file_id}}.",
        "x" = paste0(
          field_name,
          " cannot be safely cast to Date for column ",
          column_name
        )
      ))
    }
  }

  return(invisible(TRUE))
}

#' @title Validate canonical rules
#' @description Validates schema completeness, dataset-column presence, rule-key
#' uniqueness, conflict-free mappings, and type compatibility.
#' @param rules_dt Canonical rule table.
#' @param dataset_dt Dataset to mutate.
#' @param rule_file_id Character scalar rule file identifier.
#' @param stage_name Character scalar execution stage label.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_data_frame assert_string
validate_canonical_rules <- function(rules_dt, dataset_dt, rule_file_id, stage_name) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  required_columns <- get_canonical_rule_columns(validated_stage_name)
  missing_rule_columns <- setdiff(required_columns, colnames(rules_dt))
  if (length(missing_rule_columns) > 0) {
    cli::cli_abort(c(
      "Canonical rule schema validation failed for {.file {rule_file_id}}.",
      "x" = paste(missing_rule_columns, collapse = ", ")
    ))
  }

  if (nrow(rules_dt) == 0) {
    return(invisible(TRUE))
  }

  validation_context <- normalize_rule_values_for_validation(
    rules_dt = rules_dt,
    stage_name = validated_stage_name
  )
  rules_for_validation <- validation_context$rules_for_validation
  allowed_na_columns <- validation_context$allowed_na_columns

  strict_required_columns <- setdiff(required_columns, allowed_na_columns)
  columns_with_na <- strict_required_columns[vapply(
    strict_required_columns,
    function(column_name) {
      anyNA(rules_dt[[column_name]])
    },
    logical(1)
  )]

  if (length(columns_with_na) > 0) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains missing values in required columns.",
      "x" = paste(columns_with_na, collapse = ", ")
    ))
  }

  dataset_columns <- colnames(dataset_dt)
  source_columns <- unique(trimws(as.character(rules_dt$column_source)))
  target_columns <- unique(trimws(as.character(rules_dt$column_target)))
  source_columns <- source_columns[!is.na(source_columns) & nzchar(source_columns)]
  target_columns <- target_columns[!is.na(target_columns) & nzchar(target_columns)]

  missing_source <- setdiff(source_columns, dataset_columns)
  missing_target <- setdiff(target_columns, dataset_columns)

  if (length(missing_source) > 0 || length(missing_target) > 0) {
    cli::cli_abort(c(
      "Rule columns are not present in dataset for {.file {rule_file_id}}.",
      if (length(missing_source) > 0) paste0("x source: ", paste(missing_source, collapse = ", ")),
      if (length(missing_target) > 0) paste0("x target: ", paste(missing_target, collapse = ", "))
    ))
  }

  duplicate_key_dt <- rules_for_validation[
    ,
    .N,
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][N > 1L]

  if (nrow(duplicate_key_dt) > 0) {
    cli::cli_abort(c(
      "Rule uniqueness validation failed for {.file {rule_file_id}}.",
      "x" = "Each (column_source, value_source_raw, column_target, value_target_raw) must be unique."
    ))
  }

  target_value_column <- get_stage_target_value_column(validated_stage_name)
  source_value_column <- get_stage_source_value_column(validated_stage_name)

  conflict_dt <- rules_for_validation[
    ,
    .(target_value_count = uniqueN(get(target_value_column))),
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][target_value_count > 1L]

  if (nrow(conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting rules detected in {.file {rule_file_id}}.",
      "x" = "A single source/target key maps to multiple target values."
    ))
  }

  source_conflict_dt <- rules_for_validation[
    ,
    .(source_value_count = uniqueN(get(source_value_column))),
    by = .(column_source, value_source_raw)
  ][source_value_count > 1L]

  if (nrow(source_conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting source rewrite rules detected in {.file {rule_file_id}}.",
      "x" = "A single (column_source, value_source_raw) maps to multiple source result values."
    ))
  }

  rules_dt[
    ,
    check_type_compatibility(
      dataset_dt[[column_source[1]]],
      value_source_raw,
      "value_source_raw",
      rule_file_id,
      column_name = column_source[1]
    ),
    by = column_source
  ]
  rules_dt[
    ,
    check_type_compatibility(
      dataset_dt[[column_target[1]]],
      value_target_raw,
      "value_target_raw",
      rule_file_id,
      column_name = column_target[1]
    ),
    by = column_target
  ]

  rules_with_source_result <- rules_dt[!is.na(get(source_value_column))]
  if (nrow(rules_with_source_result) > 0L) {
    rules_with_source_result[
      ,
      check_type_compatibility(
        dataset_dt[[column_source[1]]],
        get(source_value_column),
        source_value_column,
        rule_file_id,
        column_name = column_source[1]
      ),
      by = column_source
    ]
  }

  return(invisible(TRUE))
}

#' @title Build conditional dictionaries from canonical rules
#' @description Groups canonical rules by `(column_source, column_target)` and
#' sorts deterministically for reproducible execution.
#' @param rules_dt Canonical rules table.
#' @return List of grouped rule tables.
#' @importFrom checkmate assert_data_frame
build_conditional_rule_dictionary <- function(rules_dt, stage_name) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)

  if (nrow(rules_dt) == 0L) {
    return(list())
  }

  target_value_column <- get_stage_target_value_column(validated_stage_name)

  ordered_rules <- data.table::as.data.table(rules_dt)[order(
    column_source,
    column_target,
    value_source_raw,
    value_target_raw,
    get(target_value_column)
  )]

  grouped_rules <- split(
    x = ordered_rules,
    f = interaction(ordered_rules$column_source, ordered_rules$column_target, drop = TRUE),
    drop = TRUE
  )

  return(grouped_rules)
}

#' @title Encode target rule values with internal missing placeholder
#' @description Converts empty strings and missing values in target rule values
#' to an explicit internal placeholder for deterministic downstream handling.
#' @param values Atomic vector values to encode.
#' @param na_placeholder Character scalar internal missing token.
#' @return Character vector with placeholder-encoded missing values.
#' @importFrom checkmate assert_atomic assert_string
encode_target_rule_value <- function(
  values,
  na_placeholder = get_pipeline_constants()$na_placeholder
) {
  checkmate::assert_atomic(values, min.len = 0, any.missing = TRUE)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  if (length(values) == 0L) {
    return(character(0))
  }

  encoded_values <- as.character(values)
  encoded_values[trimws(encoded_values) == ""] <- na_placeholder
  encoded_values[is.na(encoded_values)] <- na_placeholder

  return(encoded_values)
}

#' @title Decode internal placeholder back to `NA_character_`
#' @description Reverts encoded missing target values to canonical
#' `NA_character_` representation before rule application.
#' @param values Character vector values to decode.
#' @param na_placeholder Character scalar internal missing token.
#' @return Character vector with placeholder decoded to `NA_character_`.
#' @importFrom checkmate assert_character assert_string
decode_target_rule_value <- function(
  values,
  na_placeholder = get_pipeline_constants()$na_placeholder
) {
  checkmate::assert_character(values, any.missing = TRUE)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  decoded_values <- values
  decoded_values[decoded_values == na_placeholder] <- NA_character_

  return(decoded_values)
}

#' @title Build deterministic matching keys with explicit NA handling
#' @description Normalizes values to comparable string keys and maps missing
#' values to an explicit internal token to guarantee deterministic NA matching
#' behavior during join operations.
#' @param values Atomic vector values to encode.
#' @param na_key Character scalar NA token used for matching keys.
#' @return Character vector key.
#' @importFrom checkmate assert_atomic assert_string
encode_rule_match_key <- function(
  values,
  na_key = get_pipeline_constants()$na_match_key
) {
  checkmate::assert_atomic(values, min.len = 0, any.missing = TRUE)
  checkmate::assert_string(na_key, min.chars = 1)

  if (length(values) == 0L) {
    return(character(0))
  }

  encoded_key <- normalize_string_impl(as.character(values))
  encoded_key[is.na(encoded_key)] <- na_key

  return(encoded_key)
}

#' @title Apply one conditional dictionary group
#' @description Executes vectorized matching and mutation for one
#' `(column_source, column_target)` group and captures structured audit records.
#' @param dataset_dt Data table to mutate.
#' @param group_rules Canonical rules for one source-target column pair.
#' @param stage_name Character scalar stage label.
#' @param dataset_name Character scalar dataset identifier.
#' @param rule_file_id Character scalar rule file identifier.
#' @param execution_timestamp_utc Character scalar execution timestamp.
#' @return List with mutated `data` and `audit` table.
#' @importFrom checkmate assert_data_table assert_data_frame assert_string
apply_conditional_rule_group <- function(
  dataset_dt,
  group_rules,
  stage_name,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(group_rules, min.rows = 1)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  target_value_column <- get_stage_target_value_column(validated_stage_name)
  source_value_column <- get_stage_source_value_column(validated_stage_name)

  group_dt <- data.table::as.data.table(group_rules)
  source_column <- group_dt$column_source[[1]]
  target_column <- group_dt$column_target[[1]]

  normalized_rules <- unique(group_dt[, .(
    column_source,
    value_source_raw,
    source_value_raw = get(source_value_column),
    column_target,
    value_target_raw,
    value_target_result_encoded = encode_target_rule_value(get(target_value_column)),
    source_key = encode_rule_match_key(value_source_raw),
    target_key = encode_rule_match_key(value_target_raw)
  )][
    ,
    `:=`(
      value_source_result = as.character(source_value_raw),
      value_target_result = decode_target_rule_value(value_target_result_encoded)
    )
  ])

  normalized_rules[trimws(value_source_result) == "", value_source_result := NA_character_]

  data.table::setindex(normalized_rules, source_key, target_key)

  source_values_pre_update <- dataset_dt[[source_column]]
  target_values_pre_update <- dataset_dt[[target_column]]

  join_input <- data.table::data.table(
    source_key = encode_rule_match_key(source_values_pre_update),
    target_key = encode_rule_match_key(target_values_pre_update)
  )

  joined_dt <- normalized_rules[
    join_input,
    on = .(source_key, target_key)
  ]

  matched_row_mask <- !is.na(joined_dt$column_source)
  source_update_mask <- matched_row_mask
  matched_rows <- as.integer(sum(matched_row_mask))

  if (matched_rows > 0L) {
    if (any(source_update_mask)) {
      dataset_dt[
        source_update_mask,
        (source_column) := joined_dt$value_source_result[source_update_mask]
      ]
    }

    dataset_dt[
      matched_row_mask,
      (target_column) := joined_dt$value_target_result[matched_row_mask]
    ]
  }

  matched_counts <- joined_dt[matched_row_mask, .(
    affected_rows = .N
  ), by = .(
    source_key,
    target_key,
    value_source_result,
    value_target_result_encoded
  )]

  audit_dt <- normalized_rules[
    matched_counts,
    on = .(
      source_key,
      target_key,
      value_source_result,
      value_target_result_encoded
    )
  ][
    ,
    .(
      dataset_name = dataset_name,
      column_source,
      value_source_raw,
      value_source_result,
      column_target,
      value_target_raw,
      value_target_result,
      affected_rows = data.table::fcoalesce(affected_rows, 0L),
      execution_timestamp_utc = execution_timestamp_utc,
      rule_file_identifier = rule_file_id,
      execution_stage = validated_stage_name
    )
  ][order(column_source, column_target, value_source_raw, value_target_raw)]

  return(list(data = dataset_dt, audit = audit_dt))
}

#' @title Apply canonical rule file payload
#' @description Executes matching and mutation in deterministic group order for a
#' single file payload.
#' @param dataset_dt Data table to mutate.
#' @param canonical_rules Canonical rules table.
#' @param stage_name Character scalar stage label.
#' @param dataset_name Character scalar dataset identifier.
#' @param rule_file_id Character scalar rule file identifier.
#' @param execution_timestamp_utc Character scalar execution timestamp.
#' @return List with mutated `data` and aggregated `audit` table.
#' @importFrom checkmate assert_data_table assert_data_frame assert_string
apply_rule_payload <- function(
  dataset_dt,
  canonical_rules,
  stage_name,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(canonical_rules, min.rows = 0)
  validated_stage_name <- validate_post_processing_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  grouped_dictionary <- build_conditional_rule_dictionary(canonical_rules, validated_stage_name)

  if (length(grouped_dictionary) == 0) {
    return(list(data = dataset_dt, audit = data.table::data.table()))
  }

  num_groups <- length(grouped_dictionary)
  audit_tables <- vector("list", num_groups)
  current_data <- dataset_dt

  for (group_index in seq_len(num_groups)) {
    group_result <- apply_conditional_rule_group(
      dataset_dt = current_data,
      group_rules = grouped_dictionary[[group_index]],
      stage_name = validated_stage_name,
      dataset_name = dataset_name,
      rule_file_id = rule_file_id,
      execution_timestamp_utc = execution_timestamp_utc
    )

    current_data <- group_result$data
    audit_tables[[group_index]] <- group_result$audit
  }

  combined_audit <- data.table::rbindlist(audit_tables, use.names = TRUE, fill = TRUE)

  return(list(data = current_data, audit = combined_audit))
}

