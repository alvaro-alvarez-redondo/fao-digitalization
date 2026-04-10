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
collect_post_processing_preflight <- function(
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

  cleaning_dir <- config$paths$data$imports$cleaning
  harmonization_dir <- config$paths$data$imports$harmonization
  audit_paths <- get_post_processing_audit_paths(config)

  checks <- list(
    cleaning_dir_exists = fs::dir_exists(cleaning_dir),
    harmonize_dir_exists = fs::dir_exists(harmonization_dir),
    templates_dir_exists = fs::dir_exists(audit_paths$templates_dir),
    diagnostics_dir_exists = fs::dir_exists(audit_paths$diagnostics_dir)
  )

  issues <- character(0)

  if (!checks$cleaning_dir_exists) {
    issues <- c(issues, "[clean stage] missing 11-clean_imports directory")
  }

  if (!checks$harmonize_dir_exists) {
    issues <- c(
      issues,
      "[harmonize stage] missing 13-harmonize_imports directory"
    )
  }

  if (!checks$templates_dir_exists) {
    issues <- c(issues, "[audit_root_dir] missing templates directory")
  }

  if (!checks$diagnostics_dir_exists) {
    issues <- c(issues, "[audit_root_dir] missing diagnostics directory")
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
      "[clean stage] invalid 11-clean_imports file naming pattern (expected prefix: clean_)"
    )
  }

  if (!checks$harmonize_pattern_ok) {
    issues <- c(
      issues,
      "[harmonize stage] invalid 13-harmonize_imports file naming pattern (expected prefix: harmonize_)"
    )
  }

  has_expected_columns <- all(expected_columns %in% dataset_columns)
  checks$has_expected_columns <- has_expected_columns

  if (!has_expected_columns) {
    issues <- c(
      issues,
      paste0(
        "[run_post_processing_pipeline] missing expected columns: ",
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
#' @param preflight_result List from `collect_post_processing_preflight()`.
#' @return Invisibly returns `TRUE`.
#' @importFrom checkmate assert_list assert_flag assert_character
assert_post_processing_preflight <- function(preflight_result) {
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

#' @title Summarize stage rules from audit table
#' @description Aggregates audit records by rule file and column pairs to produce
#' a deterministic rule summary for a single post-processing stage.
#' @param audit_dt Audit data.table from a post-processing stage.
#' @param stage_name Character scalar stage label for the summary.
#' @return `data.table` with one row per unique rule/column combination.
#' @importFrom data.table as.data.table data.table
summarize_stage_rules <- function(audit_dt, stage_name) {
  stage_audit_dt <- data.table::as.data.table(audit_dt)

  required_columns <- c(
    "rule_file_identifier",
    "column_source",
    "value_source_raw",
    "column_target",
    "value_target_raw",
    "value_target",
    "affected_rows"
  )

  missing_columns <- setdiff(required_columns, names(stage_audit_dt))
  if (length(missing_columns) > 0L) {
    stage_audit_dt[, (missing_columns) := NA_character_]
  }

  stage_audit_dt[,
    affected_rows := suppressWarnings(as.integer(affected_rows))
  ]
  stage_audit_dt[is.na(affected_rows), affected_rows := 0L]

  if (nrow(stage_audit_dt) == 0L) {
    return(data.table::data.table(
      execution_stage = character(),
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target = character(),
      affected_rows = integer()
    ))
  }

  return(stage_audit_dt[,
    .(affected_rows = as.integer(sum(affected_rows))),
    by = .(
      rule_file_identifier,
      column_source,
      value_source_raw,
      column_target,
      value_target_raw,
      value_target
    )
  ][
    order(rule_file_identifier, column_source, column_target)
  ][,
    execution_stage := stage_name
  ][,
    .(
      execution_stage,
      rule_file_identifier,
      column_source,
      value_source_raw,
      column_target,
      value_target_raw,
      value_target,
      affected_rows
    )
  ])
}

#' @title Build post-processing rule summaries
#' @description Creates stage-specific rule summaries for clean and harmonize
#' audit tables.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @return Named list with `clean_rule_summary` and `harmonize_rule_summary`
#' data.tables.
#' @importFrom checkmate assert_data_frame
build_post_processing_diagnostics <- function(
  clean_audit_dt,
  harmonize_audit_dt
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)

  clean_rule_summary <- summarize_stage_rules(clean_audit_dt, "clean")
  harmonize_rule_summary <- summarize_stage_rules(
    harmonize_audit_dt,
    "harmonize"
  )

  return(list(
    clean_rule_summary = clean_rule_summary,
    harmonize_rule_summary = harmonize_rule_summary
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
#' `audit_root_dir/post_processing_diagnostics` using a single rule-summary
#' workbook with one sheet per stage, and a separate workbook containing the
#' post-standardization aggregated rows. Also writes a final-stage row subset
#' capturing all `last_rule_wins` overwrite events.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_rows_dt Data.table of rows after post-standardization
#'   aggregation (output of `aggregate_standardized_rows()`).
#' @param final_stage_dt Final post-processing data.table/data.frame.
#' @param last_rule_wins_overwrites_dt Overwrite events table collected during
#'   post-processing rule execution.
#' @param config Named configuration list.
#' @return Named character vector containing `rule_summary` and
#'   `aggregate_standardized_rows` and `last_rule_wins_overwrites` workbook
#'   paths.
#' @importFrom checkmate assert_data_frame assert_list
#' @importFrom fs path
#' @importFrom writexl write_xlsx
persist_post_processing_audit <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  standardize_rows_dt,
  final_stage_dt,
  last_rule_wins_overwrites_dt,
  config
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(standardize_rows_dt, min.rows = 0)
  checkmate::assert_data_frame(final_stage_dt, min.rows = 0)
  checkmate::assert_data_frame(last_rule_wins_overwrites_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)

  diagnostics <- build_post_processing_diagnostics(
    clean_audit_dt = clean_audit_dt,
    harmonize_audit_dt = harmonize_audit_dt
  )

  audit_paths <- initialize_post_processing_audit_root(config)
  diagnostics_dir <- audit_paths$diagnostics_dir
  ensure_directories_exist(diagnostics_dir, recurse = TRUE)

  output_paths <- c(
    rule_summary = fs::path(
      diagnostics_dir,
      "post_processing_audit_rule_summary.xlsx"
    ),
    aggregate_standardized_rows = fs::path(
      diagnostics_dir,
      "post_processing_aggregate_standardized_rows.xlsx"
    ),
    last_rule_wins_overwrites = fs::path(
      diagnostics_dir,
      "post_processing_last_rule_wins_overwrites.xlsx"
    )
  )

  last_rule_wins_subset_dt <- build_last_rule_wins_overwrite_subset(
    final_stage_dt = final_stage_dt,
    overwrite_events_dt = last_rule_wins_overwrites_dt
  )

  writexl::write_xlsx(
    list(
      clean = data.table::as.data.table(diagnostics$clean_rule_summary),
      harmonize = data.table::as.data.table(diagnostics$harmonize_rule_summary)
    ),
    path = output_paths[["rule_summary"]]
  )

  writexl::write_xlsx(
    list(
      aggregate_standardized_rows = data.table::as.data.table(
        standardize_rows_dt
      )
    ),
    path = output_paths[["aggregate_standardized_rows"]]
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
