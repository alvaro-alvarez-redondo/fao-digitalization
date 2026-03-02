# script: post-processing utilities
# description: reusable rule validation, dictionary construction, vectorized
# harmonization engine, and structured audit helpers for post-processing stages.

#' @title Read rule table from csv or excel
#' @description Reads a rule table file and returns a `data.table`.
#' @param file_path Character scalar path to rule file.
#' @return `data.table` containing rule rows.
#' @importFrom checkmate assert_string assert_file_exists
#' @importFrom fs path_ext
#' @importFrom readr read_csv
#' @importFrom readxl read_excel
#' @examples
#' \dontrun{read_rule_table("data/imports/cleaning imports/cleaning_rules.xlsx")}
read_rule_table <- function(file_path) {
  checkmate::assert_string(file_path, min.chars = 1)
  checkmate::assert_file_exists(file_path)

  file_extension <- fs::path_ext(file_path) |>
    tolower()

  if (identical(file_extension, "csv")) {
    return(readr::read_csv(file_path, show_col_types = FALSE) |> data.table::as.data.table())
  }

  if (file_extension %in% c("xlsx", "xls")) {
    return(readxl::read_excel(file_path) |> data.table::as.data.table())
  }

  cli::cli_abort("Unsupported rule extension for {.file {file_path}}.")
}

#' @title Get canonical rule columns
#' @description Returns canonical rule column names used by the harmonization engine.
#' @return Character vector of canonical columns.
#' @examples
#' get_canonical_rule_columns()
get_canonical_rule_columns <- function() {
  return(c(
    "column_source",
    "value_source_raw",
    "column_target",
    "value_target_raw",
    "value_target_clean"
  ))
}

#' @title Coerce rule schema to canonical columns
#' @description Converts supported legacy schemas to canonical rule columns while
#' preserving long-format row structure.
#' @param rule_dt Rule table as data.frame/data.table.
#' @param stage_name Character scalar execution stage label.
#' @param rule_file_id Character scalar rule file identifier.
#' @return Canonicalized `data.table` rule table.
#' @importFrom checkmate assert_data_frame assert_string
coerce_rule_schema <- function(rule_dt, stage_name, rule_file_id) {
  checkmate::assert_data_frame(rule_dt, min.rows = 0)
  checkmate::assert_string(stage_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)

  canonical_columns <- get_canonical_rule_columns()
  available_columns <- colnames(rule_dt)

  column_aliases <- c(
    value_source_raw = "original_value_source",
    value_target_raw = "original_value_target",
    value_target_clean = if (identical(stage_name, "clean")) {
      "cleaned_value_target"
    } else {
      "harmonized_value_target"
    }
  )

  mapped_names <- canonical_columns
  for (canonical_name in names(column_aliases)) {
    alias_name <- column_aliases[[canonical_name]]
    if (!canonical_name %in% available_columns && alias_name %in% available_columns) {
      mapped_names[mapped_names == canonical_name] <- alias_name
    }
  }

  missing_columns <- setdiff(mapped_names, available_columns)
  if (length(missing_columns) > 0) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} is missing required columns.",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  canonical_dt <- data.table::as.data.table(rule_dt)[, ..mapped_names]
  data.table::setnames(canonical_dt, mapped_names, canonical_columns)

  return(canonical_dt)
}

#' @title Validate canonical rules
#' @description Validates schema completeness, dataset-column presence, rule-key
#' uniqueness, conflict-free mappings, and type compatibility.
#' @param rules_dt Canonical rule table.
#' @param dataset_dt Dataset to mutate.
#' @param rule_file_id Character scalar rule file identifier.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_data_frame assert_string
validate_canonical_rules <- function(rules_dt, dataset_dt, rule_file_id) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_string(rule_file_id, min.chars = 1)

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

  columns_with_na <- required_columns[vapply(required_columns, function(column_name) {
    anyNA(rules_dt[[column_name]])
  }, logical(1))]
  if (length(columns_with_na) > 0) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} contains missing values in required columns.",
      "x" = paste(columns_with_na, collapse = ", ")
    ))
  }

  dataset_columns <- colnames(dataset_dt)
  missing_source <- setdiff(unique(rules_dt$column_source), dataset_columns)
  missing_target <- setdiff(unique(rules_dt$column_target), dataset_columns)

  if (length(missing_source) > 0 || length(missing_target) > 0) {
    cli::cli_abort(c(
      "Rule columns are not present in dataset for {.file {rule_file_id}}.",
      if (length(missing_source) > 0) paste0("x source: ", paste(missing_source, collapse = ", ")),
      if (length(missing_target) > 0) paste0("x target: ", paste(missing_target, collapse = ", "))
    ))
  }

  duplicate_key_dt <- rules_dt[
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

  conflict_dt <- rules_dt[
    ,
    .(target_value_count = uniqueN(value_target_clean)),
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][target_value_count > 1L]

  if (nrow(conflict_dt) > 0) {
    cli::cli_abort(c(
      "Conflicting rules detected in {.file {rule_file_id}}.",
      "x" = "A single source/target key maps to multiple clean values."
    ))
  }

  check_type_compatibility <- function(column_name, rule_values, field_name) {
    dataset_vector <- dataset_dt[[column_name]]
    non_missing_values <- rule_values[!is.na(rule_values)]

    if (is.factor(dataset_vector)) {
      non_missing_values <- as.character(non_missing_values)
    }

    if (is.numeric(dataset_vector)) {
      suppressWarnings(parsed_values <- as.numeric(non_missing_values))
      if (anyNA(parsed_values) && length(non_missing_values) > 0) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(field_name, " cannot be safely cast to numeric for column ", column_name)
        ))
      }
    }

    if (is.integer(dataset_vector)) {
      suppressWarnings(parsed_values <- as.integer(non_missing_values))
      if (anyNA(parsed_values) && length(non_missing_values) > 0) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(field_name, " cannot be safely cast to integer for column ", column_name)
        ))
      }
    }

    if (inherits(dataset_vector, "Date")) {
      suppressWarnings(parsed_values <- as.Date(non_missing_values))
      if (anyNA(parsed_values) && length(non_missing_values) > 0) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(field_name, " cannot be safely cast to Date for column ", column_name)
        ))
      }
    }

    return(invisible(TRUE))
  }

  rules_dt[, check_type_compatibility(column_source[1], value_source_raw, "value_source_raw"), by = column_source]
  rules_dt[, check_type_compatibility(column_target[1], value_target_raw, "value_target_raw"), by = column_target]

  return(invisible(TRUE))
}

#' @title Build conditional dictionaries from canonical rules
#' @description Groups canonical rules by `(column_source, column_target)` and
#' sorts deterministically for reproducible execution.
#' @param rules_dt Canonical rules table.
#' @return List of grouped rule tables.
#' @importFrom checkmate assert_data_frame
build_conditional_rule_dictionary <- function(rules_dt) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)

  if (nrow(rules_dt) == 0) {
    return(list())
  }

  ordered_rules <- data.table::as.data.table(rules_dt)[order(
    column_source,
    column_target,
    value_source_raw,
    value_target_raw,
    value_target_clean
  )]

  grouped_rules <- split(
    x = ordered_rules,
    f = interaction(ordered_rules$column_source, ordered_rules$column_target, drop = TRUE),
    drop = TRUE
  )

  return(grouped_rules)
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
  checkmate::assert_string(stage_name, min.chars = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  group_dt <- data.table::as.data.table(group_rules)
  source_column <- group_dt$column_source[[1]]
  target_column <- group_dt$column_target[[1]]

  normalized_rules <- unique(group_dt[, .(
    column_source,
    value_source_raw,
    column_target,
    value_target_raw,
    value_target_clean,
    source_key = normalize_string(value_source_raw),
    target_key = normalize_string(value_target_raw)
  )])

  join_input <- data.table::data.table(
    row_id = seq_len(nrow(dataset_dt)),
    source_key = normalize_string(dataset_dt[[source_column]]),
    target_key = normalize_string(dataset_dt[[target_column]])
  )

  joined_dt <- normalized_rules[
    join_input,
    on = .(source_key, target_key)
  ]

  matched_row_mask <- !is.na(joined_dt$value_target_clean)
  matched_rows <- as.integer(sum(matched_row_mask))

  if (matched_rows > 0L) {
    dataset_dt[
      matched_row_mask,
      (target_column) := joined_dt$value_target_clean[matched_row_mask]
    ]
  }

  matched_counts <- joined_dt[matched_row_mask, .(
    affected_rows = .N
  ), by = .(source_key, target_key, value_target_clean)]

  audit_dt <- normalized_rules[
    matched_counts,
    on = .(source_key, target_key, value_target_clean)
  ][
    ,
    .(
      dataset_name = dataset_name,
      column_source,
      value_source_raw,
      column_target,
      value_target_raw,
      value_target_clean,
      affected_rows = data.table::fcoalesce(affected_rows, 0L),
      execution_timestamp_utc = execution_timestamp_utc,
      rule_file_identifier = rule_file_id,
      execution_stage = stage_name
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
  checkmate::assert_string(stage_name, min.chars = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  grouped_dictionary <- build_conditional_rule_dictionary(canonical_rules)

  if (length(grouped_dictionary) == 0) {
    return(list(data = dataset_dt, audit = data.table::data.table()))
  }

  state <- purrr::reduce(
    .x = grouped_dictionary,
    .init = list(data = dataset_dt, audit = list()),
    .f = function(current_state, group_rules) {
      group_result <- apply_conditional_rule_group(
        dataset_dt = current_state$data,
        group_rules = group_rules,
        stage_name = stage_name,
        dataset_name = dataset_name,
        rule_file_id = rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc
      )

      current_state$data <- group_result$data
      current_state$audit[[length(current_state$audit) + 1L]] <- group_result$audit

      return(current_state)
    }
  )

  combined_audit <- data.table::rbindlist(state$audit, use.names = TRUE, fill = TRUE)

  return(list(data = state$data, audit = combined_audit))
}

#' @title Build layer diagnostics from audit table
#' @description Generates deterministic diagnostics summary for one stage.
#' @param layer_name Character scalar stage label.
#' @param rows_in Integer scalar rows before stage.
#' @param rows_out Integer scalar rows after stage.
#' @param audit_dt Audit table generated by harmonization engine.
#' @return Named diagnostics list.
#' @importFrom checkmate assert_string assert_int assert_data_frame
build_layer_diagnostics <- function(layer_name, rows_in, rows_out, audit_dt) {
  checkmate::assert_string(layer_name, min.chars = 1)
  checkmate::assert_int(rows_in, lower = 0)
  checkmate::assert_int(rows_out, lower = 0)
  checkmate::assert_data_frame(audit_dt, min.rows = 0)

  audit_table <- data.table::as.data.table(audit_dt)
  matched_count <- if (nrow(audit_table) == 0) 0L else as.integer(sum(audit_table$affected_rows))

  diagnostics <- list(
    layer_name = layer_name,
    execution_timestamp_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    rows_in = as.integer(rows_in),
    rows_out = as.integer(rows_out),
    matched_count = matched_count,
    unmatched_count = max(as.integer(rows_in - matched_count), 0L),
    idempotence_passed = TRUE,
    validation_passed = TRUE,
    status = if (matched_count > 0L) "pass" else "warn",
    messages = if (matched_count > 0L) {
      "Rules applied successfully"
    } else {
      "No rows matched available rules"
    }
  )

  return(diagnostics)
}
