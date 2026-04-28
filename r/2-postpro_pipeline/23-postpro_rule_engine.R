# script: post-processing rule engine
# description: schema coercion, canonical rule validation, dictionary
# construction, vectorized matching/mutation engine, and rule payload
# application for the clean and harmonize post-processing stages.

#' @title Coerce rule schema to canonical columns
#' @description Enforces strict unified canonical schema. Strips the stage
#' prefix (e.g. `clean_` or `harmonize_`) from column names and validates
#' that the resulting columns match the canonical set.
#' @param rule_dt Rule table as data.frame/data.table.
#' @param stage_name Character scalar execution stage label.
#' @param rule_file_id Character scalar rule file identifier.
#' @return Canonicalized `data.table` rule table.
#' @importFrom checkmate assert_data_frame assert_string
coerce_rule_schema <- function(rule_dt, stage_name, rule_file_id) {
  checkmate::assert_data_frame(rule_dt, min.rows = 0)
  validated_stage_name <- validate_postpro_stage_name(stage_name)
  checkmate::assert_string(rule_file_id, min.chars = 1)

  canonical_columns <- get_canonical_rule_columns()
  stage_prefix <- paste0("^", validated_stage_name, "_")

  canonical_dt <- data.table::as.data.table(rule_dt)
  available_columns <- colnames(canonical_dt)

  normalize_columns <- sub(stage_prefix, "", available_columns)
  duplicated_normalize_columns <- normalize_columns[duplicated(
    normalize_columns
  )]

  if (length(duplicated_normalize_columns) > 0L) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains duplicate columns after stage-prefix normalization.",
      "x" = paste(unique(duplicated_normalize_columns), collapse = ", ")
    ))
  }

  data.table::setnames(canonical_dt, available_columns, normalize_columns)
  available_columns <- colnames(canonical_dt)

  source_result_column <- get_stage_source_value_column(validated_stage_name)
  source_value_column_present <- source_result_column %in% available_columns
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
  canonical_dt[, source_value_column_present := source_value_column_present]

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
  validate_postpro_stage_name(stage_name)
  checkmate::assert_string(na_placeholder, min.chars = 1)

  allowed_na_columns <- intersect(
    c(
      "value_source_raw",
      "value_source",
      "value_target_raw",
      "value_target"
    ),
    colnames(rules_dt)
  )

  rules_for_validation <- data.table::copy(data.table::as.data.table(rules_dt))

  if (length(allowed_na_columns) > 0L) {
    rules_for_validation[,
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

  referenced_columns <- unique(c(
    rules_dt$column_source,
    rules_dt$column_target
  ))
  referenced_columns <- as.character(referenced_columns)
  referenced_columns <- trimws(referenced_columns)
  referenced_columns <- referenced_columns[
    !is.na(referenced_columns) & nzchar(referenced_columns)
  ]

  if (length(referenced_columns) == 0L) {
    return(dataset_dt)
  }

  missing_columns <- referenced_columns[
    !(referenced_columns %in% existing_columns)
  ]

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
validate_canonical_rules <- function(
  rules_dt,
  dataset_dt,
  rule_file_id,
  stage_name
) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  validated_stage_name <- validate_postpro_stage_name(stage_name)

  required_columns <- get_canonical_rule_columns()
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
  source_columns <- source_columns[
    !is.na(source_columns) & nzchar(source_columns)
  ]
  target_columns <- target_columns[
    !is.na(target_columns) & nzchar(target_columns)
  ]

  missing_source <- setdiff(source_columns, dataset_columns)
  missing_target <- setdiff(target_columns, dataset_columns)

  if (length(missing_source) > 0 || length(missing_target) > 0) {
    cli::cli_abort(c(
      "Rule columns are not present in dataset for {.file {rule_file_id}}.",
      if (length(missing_source) > 0) {
        paste0("x source: ", paste(missing_source, collapse = ", "))
      },
      if (length(missing_target) > 0) {
        paste0("x target: ", paste(missing_target, collapse = ", "))
      }
    ))
  }

  duplicate_key_dt <- rules_for_validation[,
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

  conflict_dt <- rules_for_validation[,
    .(target_value_count = data.table::uniqueN(get(target_value_column))),
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][target_value_count > 1L]

  if (nrow(conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting rules detected in {.file {rule_file_id}}.",
      "x" = "A single source/target key maps to multiple target values."
    ))
  }

  source_conflict_dt <- rules_for_validation[,
    .(source_value_count = data.table::uniqueN(get(source_value_column))),
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][source_value_count > 1L]

  if (nrow(source_conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting source rewrite rules detected in {.file {rule_file_id}}.",
      "x" = "A single (column_source, value_source_raw, column_target, value_target_raw) maps to multiple source result values."
    ))
  }

  rules_dt[,
    check_type_compatibility(
      dataset_dt[[column_source[1]]],
      value_source_raw,
      "value_source_raw",
      rule_file_id,
      column_name = column_source[1]
    ),
    by = column_source
  ]
  rules_dt[,
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
    rules_with_source_result[,
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
  validated_stage_name <- validate_postpro_stage_name(stage_name)

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
    f = interaction(
      ordered_rules$column_source,
      ordered_rules$column_target,
      drop = TRUE
    ),
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
  na_key = get_pipeline_constants()$na_match_key,
  apply_normalization = TRUE
) {
  checkmate::assert_atomic(values, min.len = 0, any.missing = TRUE)
  checkmate::assert_string(na_key, min.chars = 1)
  checkmate::assert_flag(apply_normalization)

  if (length(values) == 0L) {
    return(character(0))
  }

  encoded_key <- as.character(values)
  if (isTRUE(apply_normalization)) {
    encoded_key <- normalize_string(values)
  }
  encoded_key[is.na(encoded_key)] <- na_key

  return(encoded_key)
}

#' @title Resolve rule match normalization settings
#' @description Returns centralized settings controlling when match-key
#' normalization is applied.
#' @return Named list with `apply_once_before_stage`, `apply_each_pass`, and
#' `excluded_columns`.
resolve_rule_match_normalization_settings <- function() {
  settings <- get_pipeline_constants()$postpro$rule_match_normalization
  checkmate::assert_list(settings, min.len = 1)

  apply_once_before_stage <- isTRUE(settings$apply_once_before_stage)
  apply_each_pass <- isTRUE(settings$apply_each_pass)
  excluded_columns <- settings$excluded_columns

  if (is.null(excluded_columns)) {
    excluded_columns <- character(0)
  }
  checkmate::assert_character(excluded_columns, any.missing = FALSE)

  return(list(
    apply_once_before_stage = apply_once_before_stage,
    apply_each_pass = apply_each_pass,
    excluded_columns = excluded_columns
  ))
}

#' @title Empty last-rule-wins overwrite events table
#' @description Returns a standardized empty table used to collect overwrite
#' diagnostics triggered by the `last_rule_wins` strategy.
#' @return Empty `data.table` with overwrite event columns.
empty_last_rule_wins_overwrite_events_dt <- function() {
  return(data.table::data.table(
    dataset_name = character(),
    execution_stage = character(),
    rule_file_identifier = character(),
    column_source = character(),
    column_target = character(),
    row_id = integer(),
    candidate_count = integer(),
    unique_candidate_count = integer(),
    selected_value = character(),
    candidate_values = character()
  ))
}

#' @title Get target-update strategy configuration
#' @description Validates and returns centralized target-update strategies used
#' by post-processing rule application.
#' @return Named list with default strategy, supported strategies,
#' concatenate delimiter, and optional per-column overrides.
get_target_update_strategy_config <- function() {
  strategy_config <- get_pipeline_constants()$postpro$target_update_strategies

  if (is.null(strategy_config)) {
    cli::cli_abort(c(
      "missing target-update strategy configuration in pipeline constants.",
      "x" = "expected get_pipeline_constants()$postpro$target_update_strategies"
    ))
  }

  checkmate::assert_list(strategy_config, min.len = 1)
  checkmate::assert_string(strategy_config$default, min.chars = 1)
  checkmate::assert_character(
    strategy_config$supported,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE
  )
  checkmate::assert_string(
    strategy_config$concatenate_delimiter,
    min.chars = 1
  )

  if (!(strategy_config$default %in% strategy_config$supported)) {
    cli::cli_abort(c(
      "invalid target-update strategy configuration.",
      "x" = "default strategy is not listed in supported strategies"
    ))
  }

  by_column <- strategy_config$by_column
  if (is.null(by_column)) {
    by_column <- character(0)
  }

  if (is.list(by_column)) {
    by_column <- unlist(by_column, recursive = FALSE, use.names = TRUE)
  }

  checkmate::assert_character(by_column, any.missing = FALSE)

  if (
    length(by_column) > 0L &&
      (is.null(names(by_column)) || any(!nzchar(trimws(names(by_column)))))
  ) {
    cli::cli_abort(
      "target-update column overrides must be a named character vector"
    )
  }

  strategy_config$by_column <- by_column

  return(strategy_config)
}

#' @title Resolve target-update strategy for one column
#' @description Returns the configured strategy for a target column,
#' falling back to the centralized default strategy.
#' @param target_column Character scalar target column.
#' @param strategy_config Named strategy configuration list.
#' @return Character scalar strategy name.
resolve_target_update_strategy <- function(
  target_column,
  strategy_config = get_target_update_strategy_config()
) {
  checkmate::assert_string(target_column, min.chars = 1)
  checkmate::assert_list(strategy_config, min.len = 1)

  resolved_strategy <- strategy_config$default

  if (
    length(strategy_config$by_column) > 0L &&
      target_column %in% names(strategy_config$by_column)
  ) {
    resolved_strategy <- unname(strategy_config$by_column[[target_column]])
  }

  if (!(resolved_strategy %in% strategy_config$supported)) {
    cli::cli_abort(c(
      "unsupported target-update strategy configured.",
      "x" = paste0(
        "column: ",
        target_column,
        "; strategy: ",
        resolved_strategy,
        "; supported: ",
        paste(strategy_config$supported, collapse = ", ")
      )
    ))
  }

  return(resolved_strategy)
}

#' @title Resolve unique-row fast-path toggle for last-rule-wins
#' @description Returns whether the unique-row direct-update fast path is
#' enabled for `last_rule_wins` target updates.
#' @return Logical scalar fast-path toggle.
resolve_last_rule_wins_unique_row_fast_path_enabled <- function() {
  fast_path_config <- get_pipeline_constants()$postpro$target_update_fast_path

  if (!is.list(fast_path_config)) {
    return(FALSE)
  }

  return(isTRUE(fast_path_config$last_rule_wins_unique_row_id))
}

#' @title Resolve tokenized target-condition columns
#' @description Returns columns whose target-condition matching should treat
#' semicolon-delimited values as token sets. This is enabled for concatenate
#' strategy columns and always for `footnotes`.
#' @param strategy_config Named strategy configuration list.
#' @return Character vector of tokenized target-condition columns.
resolve_tokenized_target_condition_columns <- function(
  strategy_config = get_target_update_strategy_config()
) {
  checkmate::assert_list(strategy_config, min.len = 1)

  by_column <- strategy_config$by_column
  if (is.null(by_column)) {
    by_column <- character(0)
  }

  if (is.list(by_column)) {
    by_column <- unlist(by_column, recursive = FALSE, use.names = TRUE)
  }

  checkmate::assert_character(by_column, any.missing = FALSE)

  concatenate_columns <- character(0)
  if (length(by_column) > 0L && !is.null(names(by_column))) {
    concatenate_columns <- names(by_column)[by_column == "concatenate"]
  }

  return(sort(unique(c(concatenate_columns, "footnotes"))))
}

#' @title Match rule target conditions against dataset values
#' @description Matches rule target-condition values against current dataset
#' target values. For tokenized columns, semicolon-delimited current values are
#' matched by token membership while preserving exact full-string matching.
#' Wildcards for tokenized columns are explicit and controlled by
#' `get_pipeline_constants()$postpro$rule_match_wildcard_token`.
#' @param current_values Atomic vector of current dataset target values.
#' @param condition_values Atomic vector of rule target-condition values.
#' @param tokenized_target Logical scalar enabling tokenized matching.
#' @param wildcard_token Character scalar explicit wildcard token.
#' @return Logical vector of match decisions.
match_rule_target_condition_values <- function(
  current_values,
  condition_values,
  tokenized_target = FALSE,
  apply_match_normalization = TRUE,
  wildcard_token = get_pipeline_constants()$postpro$rule_match_wildcard_token
) {
  checkmate::assert_atomic(current_values, any.missing = TRUE)
  checkmate::assert_atomic(condition_values, any.missing = TRUE)
  checkmate::assert_flag(tokenized_target)
  checkmate::assert_flag(apply_match_normalization)
  checkmate::assert_string(wildcard_token, min.chars = 1)

  if (length(current_values) != length(condition_values)) {
    cli::cli_abort(
      "current and condition values must have equal length for condition matching"
    )
  }

  if (length(condition_values) == 0L) {
    return(logical(0))
  }

  condition_is_na <- is.na(condition_values)

  if (!isTRUE(tokenized_target)) {
    current_keys <- encode_rule_match_key(
      current_values,
      apply_normalization = apply_match_normalization
    )
    condition_keys <- encode_rule_match_key(
      condition_values,
      apply_normalization = apply_match_normalization
    )

    return(current_keys == condition_keys)
  }

  match_mask <- logical(length(condition_values))
  condition_chr <- as.character(condition_values)
  condition_is_wildcard <-
    !condition_is_na & trimws(condition_chr) == wildcard_token
  match_mask[condition_is_na] <- is.na(current_values[condition_is_na])
  match_mask[condition_is_wildcard] <- TRUE

  non_na_idx <- which(!condition_is_na & !condition_is_wildcard)
  if (length(non_na_idx) == 0L) {
    return(match_mask)
  }

  current_values_chr <- as.character(current_values[non_na_idx])
  condition_keys <- encode_rule_match_key(
    condition_values[non_na_idx],
    apply_normalization = apply_match_normalization
  )

  unique_current_values <- unique(current_values_chr[
    !is.na(current_values_chr)
  ])

  token_lookup <- setNames(
    lapply(unique_current_values, function(value_chr) {
      split_tokens <- strsplit(value_chr, ";", fixed = TRUE)[[1]]
      split_tokens <- trimws(split_tokens)
      split_tokens <- split_tokens[nzchar(split_tokens)]

      token_keys <- character(0)
      if (length(split_tokens) > 0L) {
        token_keys <- encode_rule_match_key(
          split_tokens,
          apply_normalization = apply_match_normalization
        )
      }

      full_key <- encode_rule_match_key(
        value_chr,
        apply_normalization = apply_match_normalization
      )

      return(unique(c(token_keys, full_key)))
    }),
    unique_current_values
  )

  for (idx_pos in seq_along(non_na_idx)) {
    out_idx <- non_na_idx[[idx_pos]]
    row_value_chr <- current_values_chr[[idx_pos]]

    if (is.na(row_value_chr)) {
      match_mask[[out_idx]] <- FALSE
      next
    }

    row_tokens <- token_lookup[[row_value_chr]]
    match_mask[[out_idx]] <- condition_keys[[idx_pos]] %in% row_tokens
  }

  return(match_mask)
}

#' @title Concatenate existing and incoming target values
#' @description Appends incoming values to existing values using a deterministic
#' delimiter while preserving missing-value semantics.
#' @param existing_values Atomic vector of current dataset values.
#' @param incoming_values Atomic vector of incoming update values.
#' @param delimiter Character scalar concatenation delimiter.
#' @return Character vector merged values.
concatenate_existing_and_incoming_values <- function(
  existing_values,
  incoming_values,
  delimiter
) {
  checkmate::assert_atomic(existing_values, any.missing = TRUE)
  checkmate::assert_atomic(incoming_values, any.missing = TRUE)
  checkmate::assert_string(delimiter, min.chars = 1)

  if (length(existing_values) != length(incoming_values)) {
    cli::cli_abort(
      "existing and incoming values must have equal length for concatenation"
    )
  }

  existing_values_norm <- as.character(existing_values)
  incoming_values_norm <- as.character(incoming_values)

  existing_values_norm[
    is.na(existing_values_norm) | trimws(existing_values_norm) == ""
  ] <- NA_character_
  incoming_values_norm[
    is.na(incoming_values_norm) | trimws(incoming_values_norm) == ""
  ] <- NA_character_

  split_deduplicate_tokens <- function(values_chr) {
    lapply(values_chr, function(single_value) {
      if (is.na(single_value)) {
        return(character(0))
      }

      split_tokens <- strsplit(single_value, ";", fixed = TRUE)[[1]]
      split_tokens <- trimws(split_tokens)
      split_tokens <- split_tokens[nzchar(split_tokens)]

      if (length(split_tokens) == 0L) {
        return(character(0))
      }

      dedup_mask <- !duplicated(split_tokens)
      return(split_tokens[dedup_mask])
    })
  }

  merged_values <- incoming_values_norm
  existing_only_mask <- !is.na(existing_values_norm) &
    is.na(incoming_values_norm)
  both_present_mask <- !is.na(existing_values_norm) &
    !is.na(incoming_values_norm)

  if (any(existing_only_mask)) {
    merged_values[existing_only_mask] <- existing_values_norm[
      existing_only_mask
    ]
  }

  if (any(both_present_mask)) {
    existing_tokens <- split_deduplicate_tokens(existing_values_norm[
      both_present_mask
    ])
    incoming_tokens <- split_deduplicate_tokens(incoming_values_norm[
      both_present_mask
    ])

    merged_values[both_present_mask] <- vapply(
      seq_along(existing_tokens),
      FUN.VALUE = character(1),
      FUN = function(idx) {
        merged_tokens <- c(existing_tokens[[idx]], incoming_tokens[[idx]])
        merged_tokens <- merged_tokens[!duplicated(merged_tokens)]
        if (length(merged_tokens) == 0L) {
          return(NA_character_)
        }
        paste(merged_tokens, collapse = delimiter)
      }
    )
  }

  return(merged_values)
}

#' @title Count element-wise value changes
#' @description Counts deterministic value changes between two same-length
#' vectors while preserving missing-value semantics.
#' @param before_values Atomic vector of values before mutation.
#' @param after_values Atomic vector of values after mutation.
#' @return Integer scalar count of changed elements.
count_elementwise_value_changes <- function(before_values, after_values) {
  checkmate::assert_atomic(before_values, any.missing = TRUE)
  checkmate::assert_atomic(after_values, any.missing = TRUE)

  if (length(before_values) != length(after_values)) {
    cli::cli_abort("before and after vectors must have equal length")
  }

  if (length(before_values) == 0L) {
    return(0L)
  }

  before_na <- is.na(before_values)
  after_na <- is.na(after_values)

  value_changed <- before_na != after_na
  comparable_mask <- !before_na & !after_na

  if (any(comparable_mask)) {
    value_changed[comparable_mask] <-
      as.character(before_values[comparable_mask]) !=
        as.character(after_values[comparable_mask])
  }

  return(as.integer(sum(value_changed)))
}

#' @title Apply target updates with strategy dispatch
#' @description Applies conditional and unconditional target updates for one
#' target column using a configured strategy (`last_rule_wins` or
#' `concatenate`).
#' @param dataset_dt Data table mutated by reference.
#' @param target_updates Data frame/data.table containing row and value updates.
#' @param target_column Character scalar target column to update.
#' @param row_id_column Character scalar row-id column in `target_updates`.
#' @param value_column Character scalar update value column in `target_updates`.
#' @param condition_column Character scalar optional target condition column.
#' @param order_columns Character vector columns used to deterministically order
#' updates before strategy reduction.
#' @return Invisible logical scalar indicating whether any update was applied.
apply_target_updates_with_strategy <- function(
  dataset_dt,
  target_updates,
  target_column,
  row_id_column = "row_id",
  value_column = "value_target_result",
  condition_column = "value_target_raw",
  order_columns = character(0),
  apply_condition_match = TRUE,
  dataset_name,
  execution_stage,
  rule_file_identifier,
  source_column
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(target_updates, min.rows = 0)
  checkmate::assert_string(target_column, min.chars = 1)
  checkmate::assert_string(row_id_column, min.chars = 1)
  checkmate::assert_string(value_column, min.chars = 1)
  checkmate::assert_string(condition_column, min.chars = 1)
  checkmate::assert_character(order_columns, any.missing = FALSE)
  checkmate::assert_flag(apply_condition_match)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(execution_stage, min.chars = 1)
  checkmate::assert_string(rule_file_identifier, min.chars = 1)
  checkmate::assert_string(source_column, min.chars = 1)

  empty_events <- empty_last_rule_wins_overwrite_events_dt()

  if (nrow(target_updates) == 0L) {
    return(list(
      applied = FALSE,
      overwrite_events = empty_events,
      changed_value_count = 0L
    ))
  }

  if (!(target_column %in% colnames(dataset_dt))) {
    cli::cli_abort(
      "target column {.val {target_column}} is missing in dataset"
    )
  }

  updates_dt <- data.table::as.data.table(data.table::copy(target_updates))

  required_columns <- c(row_id_column, value_column, condition_column)
  missing_columns <- setdiff(required_columns, colnames(updates_dt))

  if (length(missing_columns) > 0L) {
    cli::cli_abort(c(
      "target updates are missing required columns.",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  present_order_columns <- intersect(order_columns, colnames(updates_dt))
  if (length(present_order_columns) > 0L) {
    data.table::setorderv(updates_dt, cols = present_order_columns)
  }

  updates_dt[, row_id_internal := as.integer(get(row_id_column))]
  updates_dt <- updates_dt[!is.na(row_id_internal)]

  if (nrow(updates_dt) == 0L) {
    return(list(
      applied = FALSE,
      overwrite_events = empty_events,
      changed_value_count = 0L
    ))
  }

  out_of_bounds_mask <-
    updates_dt$row_id_internal < 1L |
    updates_dt$row_id_internal > nrow(dataset_dt)

  if (any(out_of_bounds_mask)) {
    cli::cli_abort(
      "target updates contain row indexes outside dataset boundaries"
    )
  }

  strategy_config <- get_target_update_strategy_config()
  tokenized_target_condition_columns <- resolve_tokenized_target_condition_columns(
    strategy_config = strategy_config
  )

  if (isTRUE(apply_condition_match)) {
    has_condition <- !is.na(updates_dt[[condition_column]])
    if (any(has_condition)) {
      conditioned_updates_raw <- updates_dt[has_condition]
      current_values <- dataset_dt[[target_column]][
        conditioned_updates_raw$row_id_internal
      ]
      condition_matches <- match_rule_target_condition_values(
        current_values = current_values,
        condition_values = conditioned_updates_raw[[condition_column]],
        tokenized_target = target_column %in% tokenized_target_condition_columns
      )

      conditioned_updates <- conditioned_updates_raw[condition_matches]

      is_wildcard_condition <- !is.na(conditioned_updates[[condition_column]]) &
        trimws(as.character(conditioned_updates[[condition_column]])) ==
          strategy_config$rule_match_wildcard_token

      if (any(is_wildcard_condition)) {
        wildcard_idx <- which(is_wildcard_condition)
        wildcard_current_values <- dataset_dt[[target_column]][
          conditioned_updates$row_id_internal[wildcard_idx]
        ]
        wildcard_candidate_values <- conditioned_updates[[value_column]][
          wildcard_idx
        ]

        wildcard_value_already_present <- match_rule_target_condition_values(
          current_values = wildcard_current_values,
          condition_values = wildcard_candidate_values,
          tokenized_target = target_column %in%
            tokenized_target_condition_columns
        )

        if (any(wildcard_value_already_present)) {
          conditioned_updates <- conditioned_updates[
            -wildcard_idx[wildcard_value_already_present]
          ]
        }
      }

      unconditional_updates <- updates_dt[!has_condition]

      updates_dt <- data.table::rbindlist(
        list(unconditional_updates, conditioned_updates),
        use.names = TRUE,
        fill = TRUE
      )
    }
  }

  if (nrow(updates_dt) == 0L) {
    return(list(
      applied = FALSE,
      overwrite_events = empty_events,
      changed_value_count = 0L
    ))
  }

  strategy <- resolve_target_update_strategy(
    target_column = target_column,
    strategy_config = strategy_config
  )

  if (identical(strategy, "last_rule_wins")) {
    updates_dt[, update_value := as.character(get(value_column))]

    use_unique_row_fast_path <-
      resolve_last_rule_wins_unique_row_fast_path_enabled() &&
      data.table::uniqueN(updates_dt$row_id_internal) == nrow(updates_dt)

    if (use_unique_row_fast_path) {
      previous_values <- dataset_dt[[target_column]][updates_dt$row_id_internal]

      data.table::set(
        dataset_dt,
        i = updates_dt$row_id_internal,
        j = target_column,
        value = updates_dt$update_value
      )

      changed_value_count <- count_elementwise_value_changes(
        before_values = previous_values,
        after_values = dataset_dt[[target_column]][updates_dt$row_id_internal]
      )

      return(list(
        applied = TRUE,
        overwrite_events = empty_events,
        changed_value_count = changed_value_count
      ))
    }

    updates_collapsed <- updates_dt[,
      .(
        update_value = update_value[.N],
        candidate_count = .N,
        unique_candidate_count = data.table::uniqueN(update_value),
        candidate_values = paste(update_value, collapse = "; ")
      ),
      by = .(row_id_internal)
    ]

    overwrite_events <- updates_collapsed[
      candidate_count > 1L & unique_candidate_count > 1L,
      .(
        dataset_name = dataset_name,
        execution_stage = execution_stage,
        rule_file_identifier = rule_file_identifier,
        column_source = source_column,
        column_target = target_column,
        row_id = as.integer(row_id_internal),
        candidate_count = as.integer(candidate_count),
        unique_candidate_count = as.integer(unique_candidate_count),
        selected_value = as.character(update_value),
        candidate_values = as.character(candidate_values)
      )
    ]

    previous_values <- dataset_dt[[target_column]][
      updates_collapsed$row_id_internal
    ]

    data.table::set(
      dataset_dt,
      i = updates_collapsed$row_id_internal,
      j = target_column,
      value = updates_collapsed$update_value
    )

    changed_value_count <- count_elementwise_value_changes(
      before_values = previous_values,
      after_values = dataset_dt[[target_column]][
        updates_collapsed$row_id_internal
      ]
    )

    return(list(
      applied = TRUE,
      overwrite_events = overwrite_events,
      changed_value_count = changed_value_count
    ))
  }

  if (identical(strategy, "concatenate")) {
    target_vector <- dataset_dt[[target_column]]
    if (!(is.character(target_vector) || is.factor(target_vector))) {
      cli::cli_abort(c(
        "concatenate strategy requires a character-like target column.",
        "x" = paste0(
          "column ",
          target_column,
          " has class: ",
          paste(class(target_vector), collapse = ", ")
        )
      ))
    }

    updates_dt[, update_value := as.character(get(value_column))]
    updates_dt[trimws(update_value) == "", update_value := NA_character_]
    updates_dt <- updates_dt[!is.na(update_value)]

    if (nrow(updates_dt) == 0L) {
      return(list(
        applied = FALSE,
        overwrite_events = empty_events,
        changed_value_count = 0L
      ))
    }

    delimiter <- strategy_config$concatenate_delimiter

    updates_collapsed <- updates_dt[,
      .(update_value = paste(update_value, collapse = delimiter)),
      by = .(row_id_internal)
    ]

    existing_values <- dataset_dt[[target_column]][
      updates_collapsed$row_id_internal
    ]
    merged_values <- concatenate_existing_and_incoming_values(
      existing_values = existing_values,
      incoming_values = updates_collapsed$update_value,
      delimiter = delimiter
    )

    data.table::set(
      dataset_dt,
      i = updates_collapsed$row_id_internal,
      j = target_column,
      value = merged_values
    )

    changed_value_count <- count_elementwise_value_changes(
      before_values = existing_values,
      after_values = dataset_dt[[target_column]][
        updates_collapsed$row_id_internal
      ]
    )

    return(list(
      applied = TRUE,
      overwrite_events = empty_events,
      changed_value_count = changed_value_count
    ))
  }

  cli::cli_abort(
    "unhandled target-update strategy {.val {strategy}} for {.val {target_column}}"
  )
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
  execution_timestamp_utc,
  apply_match_normalization = TRUE
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(group_rules, min.rows = 1)
  validated_stage_name <- validate_postpro_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)
  checkmate::assert_flag(apply_match_normalization)

  target_value_column <- get_stage_target_value_column(validated_stage_name)
  source_value_column <- get_stage_source_value_column(validated_stage_name)
  rule_match_normalization_settings <- resolve_rule_match_normalization_settings()
  excluded_columns <- rule_match_normalization_settings$excluded_columns

  group_dt <- data.table::as.data.table(group_rules)
  source_value_column_present <- source_value_column %in% names(group_dt)

  if (!(source_value_column %in% names(group_dt))) {
    group_dt[, (source_value_column) := NA_character_]
  }

  if (!("source_value_column_present" %in% names(group_dt))) {
    group_dt[, source_value_column_present := source_value_column_present]
  }

  source_column <- group_dt$column_source[[1]]
  target_column <- group_dt$column_target[[1]]
  apply_source_match_normalization <-
    isTRUE(apply_match_normalization) && !(source_column %in% excluded_columns)
  apply_target_condition_normalization <-
    isTRUE(apply_match_normalization) && !(target_column %in% excluded_columns)

  normalize_rules <- unique(group_dt[, .(
    column_source,
    value_source_raw,
    source_value_raw = get(source_value_column),
    source_value_column_present,
    column_target,
    value_target_raw,
    value_target_result_encoded = encode_target_rule_value(get(
      target_value_column
    )),
    source_key = encode_rule_match_key(
      value_source_raw,
      apply_normalization = apply_source_match_normalization
    ),
    target_key = encode_rule_match_key(
      value_target_raw,
      apply_normalization = apply_target_condition_normalization
    )
  )][,
    `:=`(
      value_source_result = as.character(source_value_raw),
      value_target_result = decode_target_rule_value(
        value_target_result_encoded
      )
    )
  ])

  normalize_rules[
    trimws(value_source_result) == "",
    value_source_result := NA_character_
  ]

  data.table::setindex(normalize_rules, source_key)

  tokenized_target_condition_columns <- resolve_tokenized_target_condition_columns(
    strategy_config = get_target_update_strategy_config()
  )

  source_values_pre_update <- dataset_dt[[source_column]]
  target_values_pre_update <- dataset_dt[[target_column]]

  join_input <- data.table::data.table(
    row_id = seq_len(nrow(dataset_dt)),
    source_key = encode_rule_match_key(
      source_values_pre_update,
      apply_normalization = apply_source_match_normalization
    )
  )

  joined_dt <- normalize_rules[
    join_input,
    on = .(source_key),
    allow.cartesian = TRUE
  ]

  target_condition_matches <- match_rule_target_condition_values(
    current_values = target_values_pre_update[joined_dt$row_id],
    condition_values = joined_dt$value_target_raw,
    tokenized_target = target_column %in% tokenized_target_condition_columns,
    apply_match_normalization = apply_target_condition_normalization
  )

  matched_row_mask <- !is.na(joined_dt$column_source) & target_condition_matches
  source_update_mask <- matched_row_mask &
    !is.na(joined_dt$source_value_column_present) &
    as.logical(joined_dt$source_value_column_present)
  matched_rows <- as.integer(sum(matched_row_mask))
  overwrite_events_dt <- empty_last_rule_wins_overwrite_events_dt()
  source_changed_value_count <- 0L
  target_changed_value_count <- 0L

  if (matched_rows > 0L) {
    if (any(source_update_mask)) {
      source_row_ids <- joined_dt$row_id[source_update_mask]
      source_values_before <- dataset_dt[[source_column]][source_row_ids]

      data.table::set(
        dataset_dt,
        i = source_row_ids,
        j = source_column,
        value = joined_dt$value_source_result[source_update_mask]
      )

      source_changed_value_count <- count_elementwise_value_changes(
        before_values = source_values_before,
        after_values = dataset_dt[[source_column]][source_row_ids]
      )
    }

    target_updates <- joined_dt[
      matched_row_mask,
      .(
        row_id,
        value_target_raw,
        value_target_result
      )
    ]

    update_result <- apply_target_updates_with_strategy(
      dataset_dt = dataset_dt,
      target_updates = target_updates,
      target_column = target_column,
      row_id_column = "row_id",
      value_column = "value_target_result",
      condition_column = "value_target_raw",
      order_columns = c("row_id"),
      apply_condition_match = FALSE,
      dataset_name = dataset_name,
      execution_stage = validated_stage_name,
      rule_file_identifier = rule_file_id,
      source_column = source_column
    )

    overwrite_events_dt <- update_result$overwrite_events
    target_changed_value_count <- update_result$changed_value_count
  }

  matched_counts <- joined_dt[
    matched_row_mask,
    .(
      affected_rows = .N
    ),
    by = .(
      source_key,
      target_key,
      value_source_result,
      value_target_result_encoded
    )
  ]

  audit_dt <- normalize_rules[
    matched_counts,
    on = .(
      source_key,
      target_key,
      value_source_result,
      value_target_result_encoded
    )
  ][,
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

  return(list(
    data = dataset_dt,
    audit = audit_dt,
    overwrite_events = overwrite_events_dt,
    changed_value_count = as.integer(
      source_changed_value_count + target_changed_value_count
    )
  ))
}

#' @title Apply footnote rules with multi-footnote split-join-reconstruct
#' @description Vectorized footnotes processing that splits semicolon-delimited
#' footnotes into long format, matches individual footnotes against rules,
#' applies replacements and removals, updates target columns from matched
#' footnotes, and reconstructs the footnotes column preserving original order.
#' @param dataset_dt Data table to mutate.
#' @param footnote_rules Canonical rules where `column_source == "footnotes"`.
#' @param stage_name Character scalar stage label.
#' @param dataset_name Character scalar dataset identifier.
#' @param rule_file_id Character scalar rule file identifier.
#' @param execution_timestamp_utc Character scalar execution timestamp.
#' @return List with mutated `data` and `audit` table compatible with
#' `apply_conditional_rule_group()` output schema.
#' @importFrom checkmate assert_data_table assert_data_frame assert_string
#' @importFrom data.table data.table as.data.table rbindlist setindex fcoalesce
apply_footnote_rules <- function(
  dataset_dt,
  footnote_rules,
  stage_name,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc,
  apply_match_normalization = TRUE
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(footnote_rules, min.rows = 1)
  validated_stage_name <- validate_postpro_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)
  checkmate::assert_flag(apply_match_normalization)

  source_value_column <- get_stage_source_value_column(validated_stage_name)
  target_value_column <- get_stage_target_value_column(validated_stage_name)
  rule_match_normalization_settings <- resolve_rule_match_normalization_settings()
  excluded_columns <- rule_match_normalization_settings$excluded_columns
  footnote_source_normalization <-
    isTRUE(apply_match_normalization) && !("footnotes" %in% excluded_columns)

  # --- ensure footnotes column exists -----------------------------------------
  if (!("footnotes" %in% colnames(dataset_dt))) {
    dataset_dt[, footnotes := NA_character_]
  }

  footnote_values_before <- dataset_dt$footnotes

  # --- step 1: assign row identifiers ----------------------------------------
  dataset_dt[, row_id := .I]
  n_rows <- nrow(dataset_dt)

  # --- step 2: split footnotes by ";" into long format -----------------------
  fn_long <- dataset_dt[,
    .(
      footnote_raw = unlist(strsplit(
        as.character(footnotes),
        ";",
        fixed = TRUE
      )),
      footnote_index = seq_along(unlist(strsplit(
        as.character(footnotes),
        ";",
        fixed = TRUE
      )))
    ),
    by = row_id
  ]
  fn_long[, footnote := trimws(footnote_raw)]
  fn_long[trimws(footnote) == "", footnote := NA_character_]

  # handle rows with NA footnotes (no split produces empty result)
  na_rows <- dataset_dt[is.na(footnotes), .(row_id)]
  if (nrow(na_rows) > 0L) {
    na_long <- data.table::data.table(
      row_id = na_rows$row_id,
      footnote_raw = NA_character_,
      footnote_index = 1L,
      footnote = NA_character_
    )
    fn_long <- data.table::rbindlist(list(fn_long, na_long), use.names = TRUE)
    data.table::setkey(fn_long, row_id, footnote_index)
  }

  # --- step 3: normalize rules and build match keys --------------------------
  rules_dt <- data.table::as.data.table(footnote_rules)
  normalize_rules <- unique(rules_dt[, .(
    column_source = "footnotes",
    value_source_raw,
    source_value_raw = get(source_value_column),
    column_target,
    value_target_raw,
    value_target_result_encoded = encode_target_rule_value(get(
      target_value_column
    )),
    source_key = encode_rule_match_key(
      value_source_raw,
      apply_normalization = footnote_source_normalization
    )
  )][,
    `:=`(
      value_source_result = as.character(source_value_raw),
      value_target_result = decode_target_rule_value(
        value_target_result_encoded
      )
    )
  ])
  normalize_rules[
    trimws(value_source_result) == "",
    value_source_result := NA_character_
  ]
  data.table::setindex(normalize_rules, source_key)

  # --- step 4: join footnotes with rules on source key -----------------------
  fn_long[,
    source_key := encode_rule_match_key(
      footnote,
      apply_normalization = footnote_source_normalization
    )
  ]
  joined <- normalize_rules[
    fn_long,
    on = .(source_key),
    allow.cartesian = TRUE
  ]

  # --- step 5: compute footnote_final using vectorized conditional logic -----
  matched_mask <- !is.na(joined$column_source)

  conditional_target_mask <- matched_mask &
    joined$column_target != "footnotes" &
    !is.na(joined$value_target_raw)

  if (any(conditional_target_mask)) {
    condition_match_mask <- rep(FALSE, nrow(joined))

    tokenized_target_condition_columns <- resolve_tokenized_target_condition_columns(
      strategy_config = get_target_update_strategy_config()
    )

    conditional_target_columns <- unique(joined$column_target[
      conditional_target_mask
    ])

    for (target_column in conditional_target_columns) {
      target_column_mask <-
        conditional_target_mask & joined$column_target == target_column

      current_target_values <- dataset_dt[[target_column]][
        joined$row_id[target_column_mask]
      ]

      condition_match_mask[target_column_mask] <-
        match_rule_target_condition_values(
          current_values = current_target_values,
          condition_values = joined$value_target_raw[target_column_mask],
          tokenized_target = target_column %in%
            tokenized_target_condition_columns,
          apply_match_normalization = isTRUE(apply_match_normalization) &&
            !(target_column %in% excluded_columns)
        )
    }

    matched_mask <- matched_mask &
      (!conditional_target_mask | condition_match_mask)
  }

  joined[, footnote_final := footnote]

  # matched replacement: value_source_result is not NA → replace footnote text
  replace_mask <- matched_mask & !is.na(joined$value_source_result)
  if (any(replace_mask)) {
    joined[replace_mask, footnote_final := value_source_result]
  }

  # matched removal: value_source_result is NA → remove footnote
  remove_mask <- matched_mask & is.na(joined$value_source_result)
  if (any(remove_mask)) {
    joined[remove_mask, footnote_final := NA_character_]
  }

  joined[, `:=`(
    is_remove = remove_mask,
    is_replace = replace_mask
  )]

  # --- step 6: apply target column updates -----------------------------------
  target_updates <- joined[
    matched_mask & column_target != "footnotes",
    .(
      row_id,
      footnote_index,
      column_target,
      value_target_raw,
      value_target_result
    )
  ]
  overwrite_event_tables <- list()
  total_target_changed_value_count <- 0L

  if (nrow(target_updates) > 0L) {
    target_columns <- unique(target_updates$column_target)

    for (tc in target_columns) {
      update_result <- apply_target_updates_with_strategy(
        dataset_dt = dataset_dt,
        target_updates = target_updates[column_target == tc],
        target_column = tc,
        row_id_column = "row_id",
        value_column = "value_target_result",
        condition_column = "value_target_raw",
        order_columns = c("row_id", "footnote_index"),
        dataset_name = dataset_name,
        execution_stage = validated_stage_name,
        rule_file_identifier = rule_file_id,
        source_column = "footnotes"
      )

      if (nrow(update_result$overwrite_events) > 0L) {
        overwrite_event_tables[[length(overwrite_event_tables) + 1L]] <-
          update_result$overwrite_events
      }

      total_target_changed_value_count <-
        total_target_changed_value_count + update_result$changed_value_count
    }
  }

  overwrite_events_dt <- if (length(overwrite_event_tables) > 0L) {
    data.table::rbindlist(overwrite_event_tables, use.names = TRUE, fill = TRUE)
  } else {
    empty_last_rule_wins_overwrite_events_dt()
  }

  # --- step 7: reconstruct footnotes per row ---------------------------------
  # Resolve each token deterministically across cartesian duplicates:
  # remove beats replace, replace beats unchanged original token.
  token_resolution <- joined[,
    .(
      footnote_final = {
        if (any(is_remove, na.rm = TRUE)) {
          NA_character_
        } else if (any(is_replace, na.rm = TRUE)) {
          replacement_values <- footnote_final[
            is_replace & !is.na(footnote_final)
          ]
          if (length(replacement_values) == 0L) {
            NA_character_
          } else {
            replacement_values[[1L]]
          }
        } else {
          original_values <- footnote[!is.na(footnote)]
          if (length(original_values) == 0L) {
            NA_character_
          } else {
            original_values[[1L]]
          }
        }
      }
    ),
    by = .(row_id, footnote_index)
  ]
  data.table::setorder(token_resolution, row_id, footnote_index)
  reconstructed <- token_resolution[,
    .(
      footnotes_new = {
        valid <- footnote_final[!is.na(footnote_final)]
        if (length(valid) == 0L) {
          NA_character_
        } else {
          paste(valid, collapse = "; ")
        }
      }
    ),
    by = row_id
  ]

  # update footnotes in dataset
  dataset_dt[reconstructed, footnotes := i.footnotes_new, on = "row_id"]

  # rows without any footnote entries (should not happen, but safety net)
  missing_recon <- setdiff(seq_len(n_rows), reconstructed$row_id)
  if (length(missing_recon) > 0L) {
    data.table::set(
      dataset_dt,
      i = missing_recon,
      j = "footnotes",
      value = NA_character_
    )
  }

  # --- step 8: clean temporary columns ---------------------------------------
  dataset_dt[, row_id := NULL]

  footnote_changed_value_count <- count_elementwise_value_changes(
    before_values = footnote_values_before,
    after_values = dataset_dt$footnotes
  )

  # --- step 9: generate audit records ----------------------------------------
  audit_source <- joined[matched_mask]

  if (nrow(audit_source) > 0L) {
    source_audit <- audit_source[,
      .(affected_rows = .N),
      by = .(
        value_source_raw,
        value_source_result,
        column_target,
        value_target_raw,
        value_target_result
      )
    ]

    audit_dt <- source_audit[, .(
      dataset_name = dataset_name,
      column_source = "footnotes",
      value_source_raw,
      value_source_result,
      column_target,
      value_target_raw,
      value_target_result,
      affected_rows = as.integer(affected_rows),
      execution_timestamp_utc = execution_timestamp_utc,
      rule_file_identifier = rule_file_id,
      execution_stage = validated_stage_name
    )][order(column_source, column_target, value_source_raw, value_target_raw)]
  } else {
    audit_dt <- data.table::data.table(
      dataset_name = character(0),
      column_source = character(0),
      value_source_raw = character(0),
      value_source_result = character(0),
      column_target = character(0),
      value_target_raw = character(0),
      value_target_result = character(0),
      affected_rows = integer(0),
      execution_timestamp_utc = character(0),
      rule_file_identifier = character(0),
      execution_stage = character(0)
    )
  }

  return(list(
    data = dataset_dt,
    audit = audit_dt,
    overwrite_events = overwrite_events_dt,
    changed_value_count = as.integer(
      total_target_changed_value_count + footnote_changed_value_count
    )
  ))
}

#' @title Apply canonical rule file payload
#' @description Executes matching and mutation in deterministic group order for a
#' single file payload. Routes footnote-source rules through the specialized
#' `apply_footnote_rules()` handler for multi-footnote split-join processing.
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
  execution_timestamp_utc,
  apply_match_normalization = TRUE
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(canonical_rules, min.rows = 0)
  validated_stage_name <- validate_postpro_stage_name(stage_name)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)
  checkmate::assert_flag(apply_match_normalization)

  if (nrow(canonical_rules) == 0L) {
    return(list(
      data = dataset_dt,
      audit = data.table::data.table(),
      overwrite_events = empty_last_rule_wins_overwrite_events_dt(),
      changed_value_count = 0L
    ))
  }

  rules_dt <- data.table::as.data.table(canonical_rules)
  audit_tables <- list()
  overwrite_tables <- list()
  changed_value_count <- 0L
  current_data <- dataset_dt

  # --- route footnote-source rules through specialized handler ----------------
  footnote_mask <- rules_dt$column_source == "footnotes"
  footnote_rules <- rules_dt[footnote_mask]
  standard_rules <- rules_dt[!footnote_mask]

  if (nrow(footnote_rules) > 0L) {
    fn_result <- apply_footnote_rules(
      dataset_dt = current_data,
      footnote_rules = footnote_rules,
      stage_name = validated_stage_name,
      dataset_name = dataset_name,
      rule_file_id = rule_file_id,
      execution_timestamp_utc = execution_timestamp_utc,
      apply_match_normalization = apply_match_normalization
    )
    current_data <- fn_result$data
    audit_tables[[length(audit_tables) + 1L]] <- fn_result$audit
    changed_value_count <- changed_value_count + fn_result$changed_value_count
    if (nrow(fn_result$overwrite_events) > 0L) {
      overwrite_tables[[length(overwrite_tables) + 1L]] <-
        fn_result$overwrite_events
    }
  }

  # --- apply remaining standard rules via grouped execution -------------------
  grouped_dictionary <- build_conditional_rule_dictionary(
    standard_rules,
    validated_stage_name
  )

  if (length(grouped_dictionary) > 0L) {
    for (group_index in seq_len(length(grouped_dictionary))) {
      group_result <- apply_conditional_rule_group(
        dataset_dt = current_data,
        group_rules = grouped_dictionary[[group_index]],
        stage_name = validated_stage_name,
        dataset_name = dataset_name,
        rule_file_id = rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc,
        apply_match_normalization = apply_match_normalization
      )

      current_data <- group_result$data
      audit_tables[[length(audit_tables) + 1L]] <- group_result$audit
      changed_value_count <-
        changed_value_count + group_result$changed_value_count
      if (nrow(group_result$overwrite_events) > 0L) {
        overwrite_tables[[length(overwrite_tables) + 1L]] <-
          group_result$overwrite_events
      }
    }
  }

  combined_audit <- data.table::rbindlist(
    audit_tables,
    use.names = TRUE,
    fill = TRUE
  )

  combined_overwrite_events <- if (length(overwrite_tables) > 0L) {
    data.table::rbindlist(overwrite_tables, use.names = TRUE, fill = TRUE)
  } else {
    empty_last_rule_wins_overwrite_events_dt()
  }

  return(list(
    data = current_data,
    audit = combined_audit,
    overwrite_events = combined_overwrite_events,
    changed_value_count = as.integer(changed_value_count)
  ))
}
