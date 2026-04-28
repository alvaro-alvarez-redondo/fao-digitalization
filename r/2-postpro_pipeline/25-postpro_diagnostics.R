# script: post-processing diagnostics
# description: consumes structured audit metadata and writes deterministic
# post-processing diagnostics outputs.

#' @title Collect post-processing preflight checks
#' @description Runs deterministic preflight checks for rule directories,
#' supported filename patterns, and expected input columns.
#' @param config Named configuration list.
#' @param dataset_columns Character vector of input dataset columns.
#' @param expected_columns Character vector of required columns for current run.
#' @return Named list with `passed`, `issues`, and `checks`.
#' @importFrom checkmate assert_list assert_character
#' @importFrom fs dir_exists dir_ls
collect_postpro_preflight <- function(
  config,
  dataset_columns,
  expected_columns = c("unit", "value", "product")
) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_character(dataset_columns, any.missing = FALSE)
  checkmate::assert_character(
    expected_columns,
    any.missing = FALSE,
    min.len = 1
  )

  cleaning_dir <- config$paths$data$import$cleaning
  harmonization_dir <- config$paths$data$import$harmonization
  audit_paths <- get_postpro_output_paths(config)

  checks <- list(
    cleaning_dir_exists = fs::dir_exists(cleaning_dir),
    harmonize_dir_exists = fs::dir_exists(harmonization_dir),
    templates_dir_exists = fs::dir_exists(audit_paths$templates_dir),
    diagnostics_dir_exists = fs::dir_exists(audit_paths$diagnostics_dir)
  )

  issues <- character(0)

  if (!checks$cleaning_dir_exists) {
    issues <- c(issues, "[clean stage] missing 11-clean_import directory")
  }

  if (!checks$harmonize_dir_exists) {
    issues <- c(
      issues,
      "[harmonize stage] missing 13-harmonize_import directory"
    )
  }

  if (!checks$templates_dir_exists) {
    issues <- c(issues, "[postpro root] missing templates directory")
  }

  if (!checks$diagnostics_dir_exists) {
    issues <- c(issues, "[postpro root] missing diagnostics directory")
  }

  cleaning_files <- if (checks$cleaning_dir_exists) {
    fs::dir_ls(cleaning_dir, regexp = "\\.(xlsx|xls|csv)$", type = "file")
  } else {
    character(0)
  }

  harmonization_files <- if (checks$harmonize_dir_exists) {
    fs::dir_ls(harmonization_dir, regexp = "\\.(xlsx|xls|csv)$", type = "file")
  } else {
    character(0)
  }

  checks$cleaning_pattern_ok <- all(grepl(
    "^clean_.*\\.(xlsx|xls|csv)$",
    basename(cleaning_files)
  ))
  checks$harmonize_pattern_ok <- all(grepl(
    "^harmonize_.*\\.(xlsx|xls|csv)$",
    basename(harmonization_files)
  ))

  if (!checks$cleaning_pattern_ok) {
    issues <- c(
      issues,
      "[clean stage] invalid 11-clean_import file naming pattern (expected prefix: clean_)"
    )
  }

  if (!checks$harmonize_pattern_ok) {
    issues <- c(
      issues,
      "[harmonize stage] invalid 13-harmonize_import file naming pattern (expected prefix: harmonize_)"
    )
  }

  has_expected_columns <- all(expected_columns %in% dataset_columns)
  checks$has_expected_columns <- has_expected_columns

  if (!has_expected_columns) {
    issues <- c(
      issues,
      paste0(
        "[run_postpro_pipeline] missing expected columns: ",
        paste(setdiff(expected_columns, dataset_columns), collapse = ", ")
      )
    )
  }

  return(list(
    passed = length(issues) == 0,
    issues = issues,
    checks = checks
  ))
}

#' @title Assert post-processing preflight checks
#' @description Aborts execution with deterministic messages when preflight fails.
#' @param preflight_result List from `collect_postpro_preflight()`.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_list assert_flag assert_character
assert_postpro_preflight <- function(preflight_result) {
  checkmate::assert_list(preflight_result, min.len = 1)
  checkmate::assert_flag(preflight_result$passed)
  checkmate::assert_character(preflight_result$issues, any.missing = FALSE)

  if (!isTRUE(preflight_result$passed)) {
    cli::cli_abort(c(
      "Post-processing preflight checks failed.",
      preflight_result$issues
    ))
  }

  return(invisible(TRUE))
}

#' @title Summarize clean/harmonize audit records
#' @description Normalizes audit records into a row-level mirror of the clean or
#' harmonize rule dictionary, preserving loop and affected-row detail.
#' @param audit_dt Audit data.table from a post-processing stage.
#' @param stage_name Character scalar stage label for the summary.
#' @return `data.table` with one row per audit record.
#' @importFrom data.table as.data.table data.table
summarize_stage_rules <- function(audit_dt, stage_name) {
  stage_audit_dt <- data.table::as.data.table(audit_dt)

  if (
    !("value_source" %in% names(stage_audit_dt)) &&
      ("value_source_result" %in% names(stage_audit_dt))
  ) {
    stage_audit_dt[, value_source := value_source_result]
  }

  if (
    !("value_target" %in% names(stage_audit_dt)) &&
      ("value_target_result" %in% names(stage_audit_dt))
  ) {
    stage_audit_dt[, value_target := value_target_result]
  }

  required_columns <- c(
    "loop",
    "affected_rows",
    "rule_file_identifier",
    "column_source",
    "value_source_raw",
    "value_source",
    "column_target",
    "value_target_raw",
    "value_target"
  )

  missing_columns <- setdiff(required_columns, names(stage_audit_dt))
  if (length(missing_columns) > 0L) {
    for (column_name in missing_columns) {
      if (column_name %in% c("loop", "affected_rows")) {
        stage_audit_dt[, (column_name) := NA_integer_]
      } else {
        stage_audit_dt[, (column_name) := NA_character_]
      }
    }
  }

  stage_audit_dt[, loop := suppressWarnings(as.integer(loop))]
  stage_audit_dt[,
    affected_rows := suppressWarnings(as.integer(affected_rows))
  ]
  stage_audit_dt[is.na(affected_rows), affected_rows := 0L]

  if (nrow(stage_audit_dt) == 0L) {
    return(data.table::data.table(
      loop = integer(),
      affected_rows = integer(),
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      value_source = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target = character()
    ))
  }

  ordered_columns <- c(
    "loop",
    "affected_rows",
    "rule_file_identifier",
    "column_source",
    "value_source_raw",
    "value_source",
    "column_target",
    "value_target_raw",
    "value_target"
  )

  return(stage_audit_dt[order(
    loop,
    rule_file_identifier,
    column_source,
    column_target,
    value_source_raw,
    value_target_raw
  )][, ..ordered_columns])
}

#' @title Build stage rule catalog from clean/harmonize payloads
#' @description Flattens rule payload objects into a canonical audit-ready
#' rule catalog.
#' @param rule_payloads List returned by `load_stage_rule_payloads()`.
#' @return `data.table` with canonical rule columns.
build_stage_rule_catalog_from_payloads <- function(rule_payloads) {
  if (length(rule_payloads) == 0L) {
    return(data.table::data.table(
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      value_source = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target = character()
    ))
  }

  stage_rule_tables <- lapply(rule_payloads, function(payload) {
    raw_rules_dt <- data.table::as.data.table(payload$raw_rules)

    if (nrow(raw_rules_dt) == 0L) {
      return(raw_rules_dt[0L, ])
    }

    if (!"column_source" %in% names(raw_rules_dt)) {
      raw_rules_dt[, column_source := NA_character_]
    }

    if (!"column_target" %in% names(raw_rules_dt)) {
      raw_rules_dt[, column_target := NA_character_]
    }

    if (!"value_source_raw" %in% names(raw_rules_dt)) {
      if ("value_source" %in% names(raw_rules_dt)) {
        raw_rules_dt[, value_source_raw := as.character(value_source)]
      } else {
        raw_rules_dt[, value_source_raw := NA_character_]
      }
    }

    if (!"value_target_raw" %in% names(raw_rules_dt)) {
      if ("value_target" %in% names(raw_rules_dt)) {
        raw_rules_dt[, value_target_raw := as.character(value_target)]
      } else {
        raw_rules_dt[, value_target_raw := NA_character_]
      }
    }

    if (!"value_source" %in% names(raw_rules_dt)) {
      raw_rules_dt[, value_source := as.character(value_source_raw)]
    }

    if (!"value_target" %in% names(raw_rules_dt)) {
      raw_rules_dt[, value_target := as.character(value_target_raw)]
    }

    raw_rules_dt[, rule_file_identifier := as.character(payload$rule_file_id)]

    rule_columns <- c(
      "rule_file_identifier",
      "column_source",
      "value_source_raw",
      "value_source",
      "column_target",
      "value_target_raw",
      "value_target"
    )

    rule_dt <- raw_rules_dt[, ..rule_columns]

    for (column_name in names(rule_dt)) {
      rule_dt[, (column_name) := as.character(get(column_name))]
      rule_dt[trimws(get(column_name)) == "", (column_name) := NA_character_]
    }

    meaningful_rule_mask <-
      !is.na(rule_dt$column_source) |
      !is.na(rule_dt$value_source_raw) |
      !is.na(rule_dt$column_target) |
      !is.na(rule_dt$value_target_raw)

    rule_dt <- rule_dt[meaningful_rule_mask]

    return(rule_dt)
  })

  combined_rule_dt <- data.table::rbindlist(stage_rule_tables, fill = TRUE)

  if (nrow(combined_rule_dt) == 0L) {
    return(data.table::data.table(
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      value_source = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target = character()
    ))
  }

  return(unique(combined_rule_dt))
}

#' @title Build standardize rule catalog from standardized layer rules
#' @description Converts standardization rules to standardize audit columns.
#' @param layer_rules_dt Standardize-layer rules table.
#' @return `data.table` with standardize audit rule columns.
build_standardize_rule_catalog <- function(layer_rules_dt) {
  if (!is.data.frame(layer_rules_dt) || nrow(layer_rules_dt) == 0L) {
    return(data.table::data.table(
      rule_file_identifier = character(),
      product_key = character(),
      unit_source = character(),
      unit_target = character(),
      unit_multiplier = numeric(),
      unit_offset = numeric()
    ))
  }

  rules_dt <- data.table::as.data.table(data.table::copy(layer_rules_dt))

  if (!"source_rule_file" %in% names(rules_dt)) {
    rules_dt[, source_rule_file := NA_character_]
  }

  if (!"unit_source" %in% names(rules_dt)) {
    rules_dt[, unit_source := NA_character_]
  }

  if (!"unit_target" %in% names(rules_dt)) {
    rules_dt[, unit_target := NA_character_]
  }

  if (!"product_key" %in% names(rules_dt)) {
    rules_dt[, product_key := NA_character_]
  }

  if (!"unit_multiplier" %in% names(rules_dt)) {
    rules_dt[, unit_multiplier := NA_real_]
  }

  if (!"unit_offset" %in% names(rules_dt)) {
    rules_dt[, unit_offset := NA_real_]
  }

  catalog_dt <- rules_dt[, .(
    rule_file_identifier = as.character(source_rule_file),
    product_key = as.character(product_key),
    unit_source = as.character(unit_source),
    unit_target = as.character(unit_target),
    unit_multiplier = as.numeric(unit_multiplier),
    unit_offset = as.numeric(unit_offset)
  )]

  character_columns <- c(
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target"
  )
  for (column_name in character_columns) {
    catalog_dt[, (column_name) := as.character(get(column_name))]
    catalog_dt[trimws(get(column_name)) == "", (column_name) := NA_character_]
  }

  meaningful_rule_mask <-
    !is.na(catalog_dt$product_key) |
    !is.na(catalog_dt$unit_source) |
    !is.na(catalog_dt$unit_target)

  return(unique(catalog_dt[meaningful_rule_mask]))
}

#' @title Summarize standardize audit records
#' @description Normalizes standardize audit records into a row-level mirror of
#' the standardization rule dictionary, preserving affected-row detail.
#' @param audit_dt Standardize audit data.table from standardize stage.
#' @return `data.table` with one row per standardize audit record.
summarize_standardize_rules <- function(audit_dt) {
  stage_audit_dt <- data.table::as.data.table(audit_dt)

  required_columns <- c(
    "affected_rows",
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target",
    "unit_multiplier",
    "unit_offset"
  )

  missing_columns <- setdiff(required_columns, names(stage_audit_dt))
  if (length(missing_columns) > 0L) {
    for (column_name in missing_columns) {
      if (column_name %in% c("affected_rows")) {
        stage_audit_dt[, (column_name) := NA_integer_]
      } else if (column_name %in% c("unit_multiplier", "unit_offset")) {
        stage_audit_dt[, (column_name) := NA_real_]
      } else {
        stage_audit_dt[, (column_name) := NA_character_]
      }
    }
  }

  stage_audit_dt[,
    affected_rows := suppressWarnings(as.integer(affected_rows))
  ]
  stage_audit_dt[is.na(affected_rows), affected_rows := 0L]
  stage_audit_dt[,
    unit_multiplier := suppressWarnings(as.numeric(unit_multiplier))
  ]
  stage_audit_dt[, unit_offset := suppressWarnings(as.numeric(unit_offset))]

  for (column_name in c(
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target"
  )) {
    stage_audit_dt[, (column_name) := as.character(get(column_name))]
    stage_audit_dt[
      trimws(get(column_name)) == "",
      (column_name) := NA_character_
    ]
  }

  if (nrow(stage_audit_dt) == 0L) {
    return(data.table::data.table(
      affected_rows = integer(),
      rule_file_identifier = character(),
      product_key = character(),
      unit_source = character(),
      unit_target = character(),
      unit_multiplier = numeric(),
      unit_offset = numeric()
    ))
  }

  ordered_columns <- c(
    "affected_rows",
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target",
    "unit_multiplier",
    "unit_offset"
  )

  return(stage_audit_dt[order(
    rule_file_identifier,
    product_key,
    unit_source,
    unit_target
  )][, ..ordered_columns])
}

#' @title Build unmatched standardize rule summary table
#' @description Computes standardization rules that never produced a successful
#' match event.
#' @param rule_catalog_dt Standardize stage rule catalog.
#' @param matched_rule_summary_dt Standardize matched-rule summary table.
#' @param matched_rule_counts_dt Optional matched-rule counts keyed by
#' `rule_product_match_key` and `unit_source_key`.
#' @return `data.table` in standardize audit schema with `affected_rows = 0`.
build_unmatched_standardize_rule_summary <- function(
  rule_catalog_dt,
  matched_rule_summary_dt,
  matched_rule_counts_dt = data.table::data.table()
) {
  if (!is.data.frame(rule_catalog_dt) || nrow(rule_catalog_dt) == 0L) {
    return(data.table::data.table(
      affected_rows = integer(),
      rule_file_identifier = character(),
      product_key = character(),
      unit_source = character(),
      unit_target = character(),
      unit_multiplier = numeric(),
      unit_offset = numeric()
    ))
  }

  rule_catalog_dt <- data.table::as.data.table(data.table::copy(
    rule_catalog_dt
  ))
  matched_rule_summary_dt <- data.table::as.data.table(data.table::copy(
    matched_rule_summary_dt
  ))
  matched_rule_counts_dt <- data.table::as.data.table(data.table::copy(
    matched_rule_counts_dt
  ))

  key_columns <- c(
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target",
    "unit_multiplier",
    "unit_offset"
  )

  for (column_name in c(
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target"
  )) {
    if (!column_name %in% names(rule_catalog_dt)) {
      rule_catalog_dt[, (column_name) := NA_character_]
    }

    if (!column_name %in% names(matched_rule_summary_dt)) {
      matched_rule_summary_dt[, (column_name) := NA_character_]
    }

    rule_catalog_dt[, (column_name) := as.character(get(column_name))]
    matched_rule_summary_dt[, (column_name) := as.character(get(column_name))]
  }

  for (column_name in c("unit_multiplier", "unit_offset")) {
    if (!column_name %in% names(rule_catalog_dt)) {
      rule_catalog_dt[, (column_name) := NA_real_]
    }

    if (!column_name %in% names(matched_rule_summary_dt)) {
      matched_rule_summary_dt[, (column_name) := NA_real_]
    }

    rule_catalog_dt[,
      (column_name) := suppressWarnings(as.numeric(get(column_name)))
    ]
    matched_rule_summary_dt[,
      (column_name) := suppressWarnings(as.numeric(get(column_name)))
    ]
  }

  rule_catalog_dt[, rule_product_match_key := normalize_string(product_key)]
  rule_catalog_dt[, unit_source_key := normalize_string(unit_source)]

  rule_key_dt <- unique(rule_catalog_dt[, ..key_columns])

  use_rule_key_counts <-
    nrow(matched_rule_counts_dt) > 0L &&
    all(
      c("rule_product_match_key", "unit_source_key") %in%
        names(matched_rule_counts_dt)
    )

  matched_key_dt <- if (use_rule_key_counts) {
    matched_rule_counts_key_dt <- unique(matched_rule_counts_dt[, .(
      rule_product_match_key = normalize_string(rule_product_match_key),
      unit_source_key = normalize_string(unit_source_key)
    )])

    unique(merge(
      rule_catalog_dt,
      matched_rule_counts_key_dt,
      by = c("rule_product_match_key", "unit_source_key"),
      all = FALSE,
      sort = FALSE
    )[, ..key_columns])
  } else {
    unique(matched_rule_summary_dt[, ..key_columns])
  }

  matched_key_dt[, matched_flag := TRUE]

  unmatched_dt <- merge(
    rule_key_dt,
    matched_key_dt,
    by = key_columns,
    all.x = TRUE,
    sort = FALSE
  )[is.na(matched_flag)]

  if (nrow(unmatched_dt) == 0L) {
    return(data.table::data.table(
      affected_rows = integer(),
      rule_file_identifier = character(),
      product_key = character(),
      unit_source = character(),
      unit_target = character(),
      unit_multiplier = numeric(),
      unit_offset = numeric()
    ))
  }

  unmatched_dt[, affected_rows := 0L]

  ordered_columns <- c(
    "affected_rows",
    "rule_file_identifier",
    "product_key",
    "unit_source",
    "unit_target",
    "unit_multiplier",
    "unit_offset"
  )

  return(unmatched_dt[order(
    rule_file_identifier,
    product_key,
    unit_source,
    unit_target
  )][, ..ordered_columns])
}

#' @title Build unmatched rule summary table
#' @description Computes rules that never produced a successful match event.
#' @param rule_catalog_dt Canonical stage rule catalog.
#' @param matched_rule_summary_dt Canonical matched-rule summary table.
#' @return `data.table` in audit summary schema with `affected_rows = 0`.
build_unmatched_rule_summary <- function(
  rule_catalog_dt,
  matched_rule_summary_dt
) {
  if (!is.data.frame(rule_catalog_dt) || nrow(rule_catalog_dt) == 0L) {
    return(data.table::data.table(
      loop = integer(),
      affected_rows = integer(),
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      value_source = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target = character()
    ))
  }

  rule_catalog_dt <- data.table::as.data.table(data.table::copy(
    rule_catalog_dt
  ))
  matched_rule_summary_dt <- data.table::as.data.table(data.table::copy(
    matched_rule_summary_dt
  ))

  key_columns <- c(
    "rule_file_identifier",
    "column_source",
    "value_source_raw",
    "column_target",
    "value_target_raw"
  )

  for (column_name in key_columns) {
    if (!column_name %in% names(rule_catalog_dt)) {
      rule_catalog_dt[, (column_name) := NA_character_]
    }

    if (!column_name %in% names(matched_rule_summary_dt)) {
      matched_rule_summary_dt[, (column_name) := NA_character_]
    }

    rule_catalog_dt[, (column_name) := as.character(get(column_name))]
    matched_rule_summary_dt[, (column_name) := as.character(get(column_name))]
  }

  rule_key_dt <- unique(rule_catalog_dt[, .(
    rule_file_identifier,
    column_source,
    value_source_raw,
    value_source,
    column_target,
    value_target_raw,
    value_target
  )])

  matched_key_dt <- unique(matched_rule_summary_dt[, ..key_columns])
  matched_key_dt[, matched_flag := TRUE]

  unmatched_dt <- merge(
    rule_key_dt,
    matched_key_dt,
    by = key_columns,
    all.x = TRUE,
    sort = FALSE
  )[is.na(matched_flag)]

  if (nrow(unmatched_dt) == 0L) {
    return(data.table::data.table(
      loop = integer(),
      affected_rows = integer(),
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      value_source = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target = character()
    ))
  }

  unmatched_dt[, `:=`(
    loop = as.integer(NA),
    affected_rows = 0L
  )]

  ordered_columns <- c(
    "loop",
    "affected_rows",
    "rule_file_identifier",
    "column_source",
    "value_source_raw",
    "value_source",
    "column_target",
    "value_target_raw",
    "value_target"
  )

  return(unmatched_dt[order(
    rule_file_identifier,
    column_source,
    column_target,
    value_source_raw,
    value_target_raw
  )][, ..ordered_columns])
}

#' @title Build post-processing rule summaries
#' @description Creates stage-specific rule summaries for clean, harmonize,
#' and standardize audit tables.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_audit_dt Standardize-stage audit table.
#' @return Named list with `clean_rule_summary`, `harmonize_rule_summary`, and
#' `standardize_rule_summary`
#' data.tables.
#' @importFrom checkmate assert_data_frame
build_postpro_diagnostics <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  standardize_audit_dt
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(standardize_audit_dt, min.rows = 0)

  clean_rule_summary <- summarize_stage_rules(clean_audit_dt, "clean")
  harmonize_rule_summary <- summarize_stage_rules(
    harmonize_audit_dt,
    "harmonize"
  )
  standardize_rule_summary <- summarize_standardize_rules(standardize_audit_dt)

  return(list(
    clean_rule_summary = clean_rule_summary,
    harmonize_rule_summary = harmonize_rule_summary,
    standardize_rule_summary = standardize_rule_summary
  ))
}

#' @title Build final-stage subset for last-rule-wins overwrites
#' @description Returns a deterministic subset of the final stage table with
#' one row per affected `row_id` where `last_rule_wins` overwrote at least one
#' prior candidate value.
#' @param final_stage_dt Final post-processing data.table/data.frame.
#' @param overwrite_events_dt Overwrite events generated during rule execution.
#' @return `data.table` with overwrite metadata and full final-stage row values.
#' @importFrom checkmate assert_data_frame
build_last_rule_wins_overwrite_subset <- function(
  final_stage_dt,
  overwrite_events_dt
) {
  checkmate::assert_data_frame(final_stage_dt, min.rows = 0)
  checkmate::assert_data_frame(overwrite_events_dt, min.rows = 0)

  final_dt <- data.table::as.data.table(data.table::copy(final_stage_dt))
  final_dt[, row_id := .I]

  metadata_empty <- data.table::data.table(
    row_id = integer(),
    overwrite_event_count = integer(),
    overwritten_columns = character(),
    overwritten_rule_files = character(),
    overwritten_stages = character()
  )

  if (nrow(final_dt) == 0L || nrow(overwrite_events_dt) == 0L) {
    return(cbind(metadata_empty, final_dt[0L, ], fill = TRUE))
  }

  events_dt <- data.table::as.data.table(data.table::copy(overwrite_events_dt))

  required_columns <- c(
    "row_id",
    "column_target",
    "rule_file_identifier",
    "execution_stage"
  )
  missing_columns <- setdiff(required_columns, names(events_dt))
  if (length(missing_columns) > 0L) {
    events_dt[, (missing_columns) := NA_character_]
  }

  events_dt[, row_id := suppressWarnings(as.integer(row_id))]
  events_dt <- events_dt[
    !is.na(row_id) & row_id >= 1L & row_id <= nrow(final_dt)
  ]

  if (nrow(events_dt) == 0L) {
    return(cbind(metadata_empty, final_dt[0L, ], fill = TRUE))
  }

  collapse_values <- function(values) {
    values_chr <- trimws(as.character(values))
    values_chr <- values_chr[!is.na(values_chr) & nzchar(values_chr)]

    if (length(values_chr) == 0L) {
      return(NA_character_)
    }

    return(paste(sort(unique(values_chr)), collapse = "; "))
  }

  row_summary <- events_dt[,
    .(
      overwrite_event_count = .N,
      overwritten_columns = collapse_values(column_target),
      overwritten_rule_files = collapse_values(rule_file_identifier),
      overwritten_stages = collapse_values(execution_stage)
    ),
    by = row_id
  ]

  row_subset <- final_dt[row_id %in% row_summary$row_id]

  output_dt <- merge(
    row_summary,
    row_subset,
    by = "row_id",
    all.x = TRUE,
    sort = TRUE
  )

  data.table::setorder(output_dt, row_id)

  return(output_dt)
}

#' @title Persist post-processing audit workbooks
#' @description Writes deterministic Excel outputs under
#' `audit_root_dir/audit` for clean/harmonize/standardize stage audits, and
#' writes a final-stage row subset under `audit_root_dir/diagnostics` capturing
#' all `last_rule_wins` overwrite events.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_audit_dt Standardize-stage audit table.
#' @param standardize_rules_dt Standardize stage rule table.
#' @param standardize_matched_rule_counts_dt Standardize-stage matched-rule
#'   counts keyed by standardized rule keys.
#' @param final_stage_dt Final post-processing data.table/data.frame.
#' @param last_rule_wins_overwrites_dt Overwrite events table collected during
#'   post-processing rule execution.
#' @param config Named configuration list.
#' @return Named character vector containing clean/harmonize/standardize and
#'   `last_rule_wins_overwrites` workbook
#'   paths.
#' @importFrom checkmate assert_data_frame assert_list
#' @importFrom fs path
#' @importFrom writexl write_xlsx
persist_postpro_audit <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  standardize_audit_dt,
  standardize_rules_dt,
  standardize_matched_rule_counts_dt = data.table::data.table(),
  final_stage_dt,
  last_rule_wins_overwrites_dt,
  config
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(standardize_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(standardize_rules_dt, min.rows = 0)
  checkmate::assert_data_frame(standardize_matched_rule_counts_dt, min.rows = 0)
  checkmate::assert_data_frame(final_stage_dt, min.rows = 0)
  checkmate::assert_data_frame(last_rule_wins_overwrites_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  diagnostics <- build_postpro_diagnostics(
    clean_audit_dt = clean_audit_dt,
    harmonize_audit_dt = harmonize_audit_dt,
    standardize_audit_dt = standardize_audit_dt
  )

  audit_paths <- initialize_postpro_output_root(config)
  audit_dir <- audit_paths$audit_dir
  diagnostics_dir <- audit_paths$diagnostics_dir
  ensure_directories_exist(audit_dir, recurse = TRUE)
  ensure_directories_exist(diagnostics_dir, recurse = TRUE)

  output_paths <- c(
    clean_audit = fs::path(
      audit_dir,
      get_pipeline_constants()$postpro$clean_audit_file_name
    ),
    harmonize_audit = fs::path(
      audit_dir,
      get_pipeline_constants()$postpro$harmonize_audit_file_name
    ),
    standardize_audit = fs::path(
      audit_dir,
      get_pipeline_constants()$postpro$standardize_audit_file_name
    ),
    last_rule_wins_overwrites = fs::path(
      diagnostics_dir,
      get_pipeline_constants()$postpro$last_rule_wins_overwrites_file_name
    )
  )

  last_rule_wins_subset_dt <- build_last_rule_wins_overwrite_subset(
    final_stage_dt = final_stage_dt,
    overwrite_events_dt = last_rule_wins_overwrites_dt
  )

  clean_rule_catalog_dt <- build_stage_rule_catalog_from_payloads(
    load_stage_rule_payloads(config = config, stage_name = "clean")
  )
  harmonize_rule_catalog_dt <- build_stage_rule_catalog_from_payloads(
    load_stage_rule_payloads(config = config, stage_name = "harmonize")
  )
  standardize_rule_catalog_dt <- build_standardize_rule_catalog(
    layer_rules_dt = standardize_rules_dt
  )

  clean_unmatched_summary <- build_unmatched_rule_summary(
    rule_catalog_dt = clean_rule_catalog_dt,
    matched_rule_summary_dt = diagnostics$clean_rule_summary
  )
  harmonize_unmatched_summary <- build_unmatched_rule_summary(
    rule_catalog_dt = harmonize_rule_catalog_dt,
    matched_rule_summary_dt = diagnostics$harmonize_rule_summary
  )
  standardize_unmatched_summary <- build_unmatched_standardize_rule_summary(
    rule_catalog_dt = standardize_rule_catalog_dt,
    matched_rule_summary_dt = diagnostics$standardize_rule_summary,
    matched_rule_counts_dt = standardize_matched_rule_counts_dt
  )

  writexl::write_xlsx(
    list(
      matched_rules = data.table::as.data.table(diagnostics$clean_rule_summary),
      unmatched_rules = data.table::as.data.table(clean_unmatched_summary)
    ),
    path = output_paths[["clean_audit"]]
  )

  writexl::write_xlsx(
    list(
      matched_rules = data.table::as.data.table(
        diagnostics$harmonize_rule_summary
      ),
      unmatched_rules = data.table::as.data.table(harmonize_unmatched_summary)
    ),
    path = output_paths[["harmonize_audit"]]
  )

  writexl::write_xlsx(
    list(
      matched_rules = data.table::as.data.table(
        diagnostics$standardize_rule_summary
      ),
      unmatched_rules = data.table::as.data.table(standardize_unmatched_summary)
    ),
    path = output_paths[["standardize_audit"]]
  )

  writexl::write_xlsx(
    list(
      last_rule_wins_overwrites = data.table::as.data.table(
        last_rule_wins_subset_dt
      )
    ),
    path = output_paths[["last_rule_wins_overwrites"]]
  )

  return(output_paths)
}
