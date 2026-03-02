# script: post-processing utilities
# description: reusable rule validation, stage-aware vectorized harmonization,
# deterministic template generation, and centralized audit path management.

#' @title Get supported post-processing stages
#' @description Returns deterministic ordered stage names.
#' @return Character vector.
get_post_processing_stages <- function() {
  return(c("clean", "harmonize"))
}

#' @title Assert supported post-processing stage
#' @description Validates stage value.
#' @param stage Character scalar stage name.
#' @return Invisibly returns `TRUE`.
assert_supported_stage <- function(stage) {
  checkmate::assert_string(stage, min.chars = 1)
  checkmate::assert_choice(stage, get_post_processing_stages())

  return(invisible(TRUE))
}

#' @title Get stage specification
#' @description Returns stage-specific settings for import directory key,
#' filename prefix, and legacy target-value alias.
#' @param stage Character scalar stage name.
#' @return Named list stage specification.
get_stage_spec <- function(stage) {
  assert_supported_stage(stage)

  spec_by_stage <- list(
    clean = list(
      imports_key = "cleaning",
      file_prefix = "cleaning",
      legacy_target_column = "cleaned_value_target"
    ),
    harmonize = list(
      imports_key = "harmonization",
      file_prefix = "harmonization",
      legacy_target_column = "harmonized_value_target"
    )
  )

  return(spec_by_stage[[stage]])
}

#' @title Get canonical rule columns
#' @description Returns canonical long-format rule columns.
#' @return Character vector.
get_canonical_rule_columns <- function() {
  return(c(
    "column_source",
    "value_source_raw",
    "column_target",
    "value_target_raw",
    "value_target_clean"
  ))
}

#' @title Read rule table
#' @description Reads csv/xls/xlsx rule file into `data.table`.
#' @param file_path Character scalar file path.
#' @return Rule table as `data.table`.
read_rule_table <- function(file_path) {
  checkmate::assert_string(file_path, min.chars = 1)
  checkmate::assert_file_exists(file_path)

  file_extension <- fs::path_ext(file_path) |> tolower()

  if (identical(file_extension, "csv")) {
    return(readr::read_csv(file = file_path, show_col_types = FALSE) |> data.table::as.data.table())
  }

  if (file_extension %in% c("xlsx", "xls")) {
    return(readxl::read_excel(path = file_path) |> data.table::as.data.table())
  }

  cli::cli_abort("Unsupported rule file extension for {.file {file_path}}")
}

#' @title Get post-processing audit paths
#' @description Builds and creates centralized audit directories under
#' `config$paths$data$audit$audit_root_dir`.
#' @param config Named configuration list.
#' @return Named list of directories.
get_post_processing_audit_paths <- function(config) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(config$paths$data$audit$audit_root_dir, min.chars = 1)

  audit_root_dir <- config$paths$data$audit$audit_root_dir
  rule_templates_dir <- file.path(audit_root_dir, "rule_templates")
  post_processing_dir <- file.path(audit_root_dir, "post_processing")
  diagnostics_dir <- file.path(audit_root_dir, "diagnostics")
  logs_dir <- file.path(audit_root_dir, "logs")

  fs::dir_create(path = c(
    audit_root_dir,
    rule_templates_dir,
    post_processing_dir,
    diagnostics_dir,
    logs_dir
  ), recurse = TRUE)

  return(list(
    audit_root_dir = audit_root_dir,
    rule_templates_dir = rule_templates_dir,
    post_processing_dir = post_processing_dir,
    diagnostics_dir = diagnostics_dir,
    logs_dir = logs_dir
  ))
}

#' @title Write rule template workbook
#' @description Writes deterministic stage template with formatted headers.
#' @param stage Character scalar stage name.
#' @param template_path Character scalar output path.
#' @param overwrite Logical scalar overwrite flag.
#' @return Character scalar template path.
write_stage_rule_template <- function(stage, template_path, overwrite = FALSE) {
  assert_supported_stage(stage)
  checkmate::assert_string(template_path, min.chars = 1)
  checkmate::assert_flag(overwrite)

  template_dt <- data.table::data.table(
    column_source = c("<source_column>"),
    value_source_raw = c("<source_value>"),
    column_target = c("<target_column>"),
    value_target_raw = c("<target_value_raw>"),
    value_target_clean = c("<target_value_clean>")
  )

  workbook <- openxlsx::createWorkbook()
  sheet_name <- paste0(stage, "_rules")
  openxlsx::addWorksheet(wb = workbook, sheetName = sheet_name)
  openxlsx::writeData(wb = workbook, sheet = sheet_name, x = template_dt)

  header_style <- openxlsx::createStyle(
    textDecoration = "bold",
    fgFill = "#DCE6F1",
    border = "Bottom"
  )

  openxlsx::addStyle(
    wb = workbook,
    sheet = sheet_name,
    style = header_style,
    rows = 1,
    cols = seq_along(colnames(template_dt)),
    gridExpand = TRUE
  )

  openxlsx::saveWorkbook(wb = workbook, file = template_path, overwrite = overwrite)

  return(template_path)
}

#' @title Ensure stage rule template exists
#' @description Creates template in centralized audit template directory if
#' missing, or rewrites when requested.
#' @param config Named configuration list.
#' @param stage Character scalar stage name.
#' @param rewrite Logical scalar rewrite flag.
#' @return Character scalar template path.
ensure_stage_rule_template <- function(config, stage, rewrite = FALSE) {
  assert_supported_stage(stage)
  checkmate::assert_flag(rewrite)

  stage_spec <- get_stage_spec(stage)
  audit_paths <- get_post_processing_audit_paths(config)

  template_path <- file.path(
    audit_paths$rule_templates_dir,
    paste0(stage_spec$file_prefix, "_rules_template.xlsx")
  )

  if (rewrite || !file.exists(template_path)) {
    write_stage_rule_template(
      stage = stage,
      template_path = template_path,
      overwrite = TRUE
    )
  }

  return(template_path)
}

#' @title List stage rule files
#' @description Returns sorted rule files for a stage.
#' @param config Named configuration list.
#' @param stage Character scalar stage name.
#' @return Character vector file paths.
list_stage_rule_files <- function(config, stage) {
  checkmate::assert_list(config, min.len = 1)
  assert_supported_stage(stage)

  stage_spec <- get_stage_spec(stage)
  imports_dir <- config$paths$data$imports[[stage_spec$imports_key]]
  checkmate::assert_string(imports_dir, min.chars = 1)

  fs::dir_create(path = imports_dir, recurse = TRUE)

  pattern <- paste0("^", stage_spec$file_prefix, "_.*\\.(xlsx|xls|csv)$")
  rule_files <- fs::dir_ls(path = imports_dir, regexp = pattern, type = "file")

  return(sort(rule_files))
}

#' @title Coerce stage rule schema to canonical columns
#' @description Supports canonical schema and legacy aliases with stage-aware
#' target-column interpretation.
#' @param rules_dt Rule table.
#' @param stage Character scalar stage name.
#' @param rule_file_id Character scalar file id for errors.
#' @return Canonicalized rule table.
coerce_rule_schema <- function(rules_dt, stage, rule_file_id) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  assert_supported_stage(stage)
  checkmate::assert_string(rule_file_id, min.chars = 1)

  stage_spec <- get_stage_spec(stage)
  canonical_columns <- get_canonical_rule_columns()

  alias_map <- c(
    value_source_raw = "original_value_source",
    value_target_raw = "original_value_target",
    value_target_clean = stage_spec$legacy_target_column
  )

  available_columns <- colnames(rules_dt)
  resolved_names <- canonical_columns

  for (canonical_name in names(alias_map)) {
    alias_name <- alias_map[[canonical_name]]
    if (!canonical_name %in% available_columns && alias_name %in% available_columns) {
      resolved_names[resolved_names == canonical_name] <- alias_name
    }
  }

  missing_columns <- setdiff(resolved_names, available_columns)
  if (length(missing_columns) > 0) {
    cli::cli_abort(c(
      "Rule file {.file {rule_file_id}} is missing required columns.",
      "x" = paste(missing_columns, collapse = ", ")
    ))
  }

  canonical_dt <- data.table::as.data.table(rules_dt)[, ..resolved_names]
  data.table::setnames(canonical_dt, old = resolved_names, new = canonical_columns)

  return(canonical_dt)
}

#' @title Validate canonical rules
#' @description Validates required columns, missingness, dataset columns,
#' uniqueness, conflict checks, and type compatibility.
#' @param rules_dt Canonical rule table.
#' @param dataset_dt Dataset.
#' @param rule_file_id Character scalar file identifier.
#' @return Invisibly returns `TRUE`.
validate_canonical_rules <- function(rules_dt, dataset_dt, rule_file_id) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_string(rule_file_id, min.chars = 1)

  required_columns <- get_canonical_rule_columns()
  missing_rule_columns <- setdiff(required_columns, colnames(rules_dt))

  if (length(missing_rule_columns) > 0) {
    cli::cli_abort(c(
      "Canonical schema validation failed for {.file {rule_file_id}}.",
      "x" = paste(missing_rule_columns, collapse = ", ")
    ))
  }

  if (nrow(rules_dt) == 0) {
    return(invisible(TRUE))
  }

  missing_required_values <- required_columns[vapply(required_columns, function(column_name) {
    anyNA(rules_dt[[column_name]])
  }, logical(1))]

  if (length(missing_required_values) > 0) {
    cli::cli_abort(c(
      "Rules contain missing values for {.file {rule_file_id}}.",
      "x" = paste(missing_required_values, collapse = ", ")
    ))
  }

  dataset_columns <- colnames(dataset_dt)
  missing_sources <- setdiff(unique(rules_dt$column_source), dataset_columns)
  missing_targets <- setdiff(unique(rules_dt$column_target), dataset_columns)

  if (length(missing_sources) > 0 || length(missing_targets) > 0) {
    cli::cli_abort(c(
      "Rules reference unknown dataset columns for {.file {rule_file_id}}.",
      if (length(missing_sources) > 0) paste0("x source: ", paste(missing_sources, collapse = ", ")),
      if (length(missing_targets) > 0) paste0("x target: ", paste(missing_targets, collapse = ", "))
    ))
  }

  duplicate_key_dt <- rules_dt[
    ,
    .N,
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][N > 1L]

  if (nrow(duplicate_key_dt) > 0) {
    cli::cli_abort(c(
      "Duplicate rules detected in {.file {rule_file_id}}.",
      "x" = "(column_source, value_source_raw, column_target, value_target_raw) must be unique."
    ))
  }

  conflicting_rules <- rules_dt[
    ,
    .(distinct_targets = data.table::uniqueN(value_target_clean)),
    by = .(column_source, value_source_raw, column_target, value_target_raw)
  ][distinct_targets > 1L]

  if (nrow(conflicting_rules) > 0) {
    cli::cli_abort(c(
      "Conflicting rules detected in {.file {rule_file_id}}.",
      "x" = "A unique source/target key maps to multiple clean values."
    ))
  }

  check_column_type <- function(column_name, raw_values, raw_field_name) {
    column_vector <- dataset_dt[[column_name]]
    non_missing_values <- raw_values[!is.na(raw_values)]

    if (is.factor(column_vector) || is.character(column_vector)) {
      return(invisible(TRUE))
    }

    if (is.numeric(column_vector)) {
      suppressWarnings(parsed <- as.numeric(non_missing_values))
      if (length(non_missing_values) > 0 && anyNA(parsed)) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(raw_field_name, " incompatible with numeric column ", column_name)
        ))
      }
    }

    if (is.integer(column_vector)) {
      suppressWarnings(parsed <- as.integer(non_missing_values))
      if (length(non_missing_values) > 0 && anyNA(parsed)) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(raw_field_name, " incompatible with integer column ", column_name)
        ))
      }
    }

    if (inherits(column_vector, "Date")) {
      suppressWarnings(parsed <- as.Date(non_missing_values))
      if (length(non_missing_values) > 0 && anyNA(parsed)) {
        cli::cli_abort(c(
          "Type compatibility validation failed for {.file {rule_file_id}}.",
          "x" = paste0(raw_field_name, " incompatible with Date column ", column_name)
        ))
      }
    }

    return(invisible(TRUE))
  }

  rules_dt[, check_column_type(column_source[[1]], value_source_raw, "value_source_raw"), by = column_source]
  rules_dt[, check_column_type(column_target[[1]], value_target_raw, "value_target_raw"), by = column_target]

  return(invisible(TRUE))
}

#' @title Build conditional dictionary
#' @description Groups canonical rules by `(column_source, column_target)` in
#' deterministic order.
#' @param rules_dt Canonical rule table.
#' @return List of grouped rule tables.
build_conditional_rule_dictionary <- function(rules_dt) {
  checkmate::assert_data_frame(rules_dt, min.rows = 0)

  if (nrow(rules_dt) == 0) {
    return(list())
  }

  ordered_dt <- data.table::as.data.table(rules_dt)[order(
    column_source,
    column_target,
    value_source_raw,
    value_target_raw,
    value_target_clean
  )]

  grouped <- split(
    x = ordered_dt,
    f = interaction(ordered_dt$column_source, ordered_dt$column_target, drop = TRUE),
    drop = TRUE
  )

  return(grouped)
}

#' @title Apply conditional dictionary group
#' @description Performs vectorized matching and mutation for one column pair
#' and returns group-level audit.
#' @param dataset_dt Dataset table.
#' @param group_rules Rule group table.
#' @param stage Character scalar stage name.
#' @param dataset_name Character scalar dataset name.
#' @param rule_file_id Character scalar rule file id.
#' @param execution_timestamp_utc Character scalar timestamp.
#' @return Named list with `data` and `audit`.
apply_conditional_rule_group <- function(
  dataset_dt,
  group_rules,
  stage,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(group_rules, min.rows = 1)
  assert_supported_stage(stage)
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

  joined <- normalized_rules[join_input, on = .(source_key, target_key)]
  match_mask <- !is.na(joined$value_target_clean)

  if (any(match_mask)) {
    dataset_dt[match_mask, (target_column) := joined$value_target_clean[match_mask]]
  }

  matched_counts <- joined[match_mask, .(
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
      execution_stage = stage
    )
  ][order(column_source, column_target, value_source_raw, value_target_raw)]

  return(list(data = dataset_dt, audit = audit_dt))
}

#' @title Apply stage rule payload
#' @description Applies one stage payload by deterministic group order.
#' @param dataset_dt Dataset table.
#' @param canonical_rules Canonical rule table.
#' @param stage Character scalar stage name.
#' @param dataset_name Character scalar dataset name.
#' @param rule_file_id Character scalar file id.
#' @param execution_timestamp_utc Character scalar timestamp.
#' @return Named list with `data` and aggregated `audit`.
apply_rule_payload <- function(
  dataset_dt,
  canonical_rules,
  stage,
  dataset_name,
  rule_file_id,
  execution_timestamp_utc
) {
  checkmate::assert_data_table(dataset_dt)
  checkmate::assert_data_frame(canonical_rules, min.rows = 0)
  assert_supported_stage(stage)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(rule_file_id, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  grouped_rules <- build_conditional_rule_dictionary(canonical_rules)

  if (length(grouped_rules) == 0) {
    return(list(data = dataset_dt, audit = data.table::data.table()))
  }

  state <- purrr::reduce(
    .x = grouped_rules,
    .init = list(data = dataset_dt, audit = list()),
    .f = function(current_state, group_rule_dt) {
      group_result <- apply_conditional_rule_group(
        dataset_dt = current_state$data,
        group_rules = group_rule_dt,
        stage = stage,
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

#' @title Run stage harmonization engine
#' @description Stage-parameterized engine for clean or harmonize execution with
#' dynamic rule interpretation.
#' @param dataset_dt Input dataset.
#' @param config Named configuration list.
#' @param stage Character scalar stage name.
#' @param dataset_name Character scalar dataset name.
#' @param rewrite_template Logical scalar rewrite-template flag.
#' @return Data table with `layer_audit` and `layer_diagnostics` attributes.
run_stage_harmonization_engine <- function(
  dataset_dt,
  config,
  stage,
  dataset_name,
  rewrite_template = FALSE
) {
  checkmate::assert_data_frame(dataset_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  assert_supported_stage(stage)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_flag(rewrite_template)

  ensure_stage_rule_template(config = config, stage = stage, rewrite = rewrite_template)

  rule_files <- list_stage_rule_files(config = config, stage = stage)
  execution_timestamp_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  initial_state <- list(
    data = data.table::copy(data.table::as.data.table(dataset_dt)),
    audit_tables = list()
  )

  final_state <- purrr::reduce(
    .x = rule_files,
    .init = initial_state,
    .f = function(state, rule_file_path) {
      rule_file_id <- fs::path_file(rule_file_path)
      raw_rules <- read_rule_table(rule_file_path)
      canonical_rules <- coerce_rule_schema(
        rules_dt = raw_rules,
        stage = stage,
        rule_file_id = rule_file_id
      )

      validate_canonical_rules(
        rules_dt = canonical_rules,
        dataset_dt = state$data,
        rule_file_id = rule_file_id
      )

      if (nrow(canonical_rules) == 0) {
        return(state)
      }

      payload_result <- apply_rule_payload(
        dataset_dt = state$data,
        canonical_rules = canonical_rules,
        stage = stage,
        dataset_name = dataset_name,
        rule_file_id = rule_file_id,
        execution_timestamp_utc = execution_timestamp_utc
      )

      state$data <- payload_result$data
      state$audit_tables[[length(state$audit_tables) + 1L]] <- payload_result$audit

      return(state)
    }
  )

  layer_audit <- data.table::rbindlist(final_state$audit_tables, use.names = TRUE, fill = TRUE)

  layer_diagnostics <- list(
    layer_name = stage,
    execution_timestamp_utc = execution_timestamp_utc,
    rows_in = nrow(dataset_dt),
    rows_out = nrow(final_state$data),
    matched_count = if (nrow(layer_audit) == 0) 0L else as.integer(sum(layer_audit$affected_rows)),
    status = if (nrow(layer_audit) == 0) "warn" else "pass"
  )

  output_dt <- final_state$data
  attr(output_dt, "layer_audit") <- layer_audit
  attr(output_dt, "layer_diagnostics") <- layer_diagnostics

  return(output_dt)
}
