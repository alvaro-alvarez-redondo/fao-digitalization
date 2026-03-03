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
  checkmate::assert_character(expected_columns, any.missing = FALSE, min.len = 1)

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
    issues <- c(issues, "[clean stage] missing clean_imports directory")
  }

  if (!checks$harmonize_dir_exists) {
    issues <- c(issues, "[harmonize stage] missing harmonize_imports directory")
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

  checks$cleaning_pattern_ok <- all(grepl("^clean_.*\\.(xlsx|xls|csv)$", basename(cleaning_files)))
  checks$harmonize_pattern_ok <- all(grepl("^harmonize_.*\\.(xlsx|xls|csv)$", basename(harmonization_files)))

  if (!checks$cleaning_pattern_ok) {
    issues <- c(issues, "[clean stage] invalid clean_imports file naming pattern (expected prefix: clean_)")
  }

  if (!checks$harmonize_pattern_ok) {
    issues <- c(issues, "[harmonize stage] invalid harmonize_imports file naming pattern (expected prefix: harmonize_)")
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

#' @title Build post-processing diagnostics summary
#' @description Creates stage and rule-level summaries from clean, standardize,
#' and harmonize diagnostics metadata.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_diagnostics Named diagnostics list for standardize layer.
#' @return Named list with `stage_summary`, `rule_summary`, and
#' `standardize_summary` data.tables.
#' @importFrom checkmate assert_data_frame assert_list
build_post_processing_diagnostics <- function(clean_audit_dt, harmonize_audit_dt, standardize_diagnostics = list()) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_list(standardize_diagnostics)

  combined_audit <- data.table::rbindlist(
    list(
      data.table::as.data.table(clean_audit_dt),
      data.table::as.data.table(harmonize_audit_dt)
    ),
    use.names = TRUE,
    fill = TRUE
  )


  required_audit_columns <- c(
    "execution_stage",
    "rule_file_identifier",
    "column_source",
    "value_source_raw",
    "column_target",
    "value_target_raw",
    "value_target_clean",
    "affected_rows"
  )

  missing_audit_columns <- setdiff(required_audit_columns, names(combined_audit))
  if (length(missing_audit_columns) > 0L) {
    for (column_name in missing_audit_columns) {
      combined_audit[, (column_name) := NA_character_]
    }
  }

  combined_audit[, affected_rows := suppressWarnings(as.integer(affected_rows))]
  combined_audit[is.na(affected_rows), affected_rows := 0L]

  stage_summary <- if (nrow(combined_audit) == 0) {
    data.table::data.table(
      execution_stage = character(),
      matched_rows = integer(),
      unmatched_rows = integer(),
      matched_rules = integer(),
      rule_files = integer(),
      potential_warnings = character()
    )
  } else {
    combined_audit[
      ,
      .(
        matched_rows = as.integer(sum(affected_rows)),
        unmatched_rows = NA_integer_,
        matched_rules = uniqueN(paste(column_source, value_source_raw, column_target, value_target_raw, sep = "|")),
        rule_files = uniqueN(rule_file_identifier),
        potential_warnings = ""
      ),
      by = .(execution_stage)
    ]
  }

  `%||%` <- function(x, y) {
    if (is.null(x)) y else x
  }

  standardized_layer <- standardize_diagnostics$standardize_units
  standardize_summary <- if (is.list(standardized_layer) && length(standardized_layer) > 0L) {
    data.table::data.table(
      execution_stage = "standardize",
      matched_rows = as.integer(standardized_layer$matched_count %||% 0L),
      unmatched_rows = as.integer(standardized_layer$unmatched_count %||% 0L),
      matched_rules = as.integer(standardized_layer$applied_rules %||% 0L),
      rule_files = as.integer(length(unique(as.character(standardized_layer$rule_sources %||% character(0))))),
      potential_warnings = if (!is.null(standardized_layer$potential_warnings)) {
        paste(as.character(standardized_layer$potential_warnings), collapse = " | ")
      } else {
        ""
      }
    )
  } else {
    data.table::data.table(
      execution_stage = "standardize",
      matched_rows = 0L,
      unmatched_rows = 0L,
      matched_rules = 0L,
      rule_files = 0L,
      potential_warnings = ""
    )
  }

  if (nrow(stage_summary) > 0L) {
    stage_summary <- data.table::rbindlist(list(stage_summary, standardize_summary), use.names = TRUE, fill = TRUE)
  } else {
    stage_summary <- standardize_summary
  }

  stage_order <- c("clean", "standardize", "harmonize")
  stage_summary <- stage_summary[order(match(execution_stage, stage_order))]

  rule_summary <- if (nrow(combined_audit) == 0L) {
    data.table::data.table(
      execution_stage = character(),
      rule_file_identifier = character(),
      column_source = character(),
      value_source_raw = character(),
      column_target = character(),
      value_target_raw = character(),
      value_target_clean = character(),
      affected_rows = integer()
    )
  } else {
    combined_audit[
      ,
      .(affected_rows = as.integer(sum(affected_rows))),
      by = .(
        execution_stage,
        rule_file_identifier,
        column_source,
        value_source_raw,
        column_target,
        value_target_raw,
        value_target_clean
      )
    ][order(execution_stage, rule_file_identifier, column_source, column_target)]
  }

  return(list(
    stage_summary = stage_summary,
    rule_summary = rule_summary,
    standardize_summary = standardize_summary
  ))
}

#' @title Persist post-processing audit workbooks
#' @description Writes deterministic single-sheet Excel outputs under
#' `audit_root_dir/post_processing_diagnostics` and overwrites them on each run.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_diagnostics Named diagnostics list for standardize layer.
#' @param dataset_name Character scalar dataset name.
#' @param execution_timestamp_utc Character scalar run timestamp (retained for backward compatibility).
#' @param config Named configuration list.
#' @return Named character vector of written workbook paths.
#' @importFrom checkmate assert_data_frame assert_string assert_list
#' @importFrom fs dir_create path
persist_post_processing_audit <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  standardize_diagnostics,
  dataset_name,
  execution_timestamp_utc,
  config
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_list(standardize_diagnostics)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)
  checkmate::assert_list(config, min.len = 1)

  diagnostics <- build_post_processing_diagnostics(
    clean_audit_dt = clean_audit_dt,
    harmonize_audit_dt = harmonize_audit_dt,
    standardize_diagnostics = standardize_diagnostics
  )

  audit_paths <- initialize_post_processing_audit_root(config)
  diagnostics_dir <- audit_paths$diagnostics_dir
  fs::dir_create(diagnostics_dir, recurse = TRUE)

  output_paths <- c(
    clean_audit = fs::path(diagnostics_dir, paste0("post_processing_audit_", dataset_name, "_clean.xlsx")),
    harmonize_audit = fs::path(diagnostics_dir, paste0("post_processing_audit_", dataset_name, "_harmonize.xlsx")),
    stage_summary = fs::path(diagnostics_dir, paste0("post_processing_audit_", dataset_name, "_stage_summary.xlsx")),
    rule_summary = fs::path(diagnostics_dir, paste0("post_processing_audit_", dataset_name, "_rule_summary.xlsx")),
    standardize_diagnostics = fs::path(diagnostics_dir, paste0("post_processing_audit_", dataset_name, "_standardize_diagnostics.xlsx"))
  )

  write_single_sheet_workbook <- function(sheet_name, sheet_data, output_path) {
    workbook <- openxlsx::createWorkbook()
    openxlsx::addWorksheet(workbook, sheet_name)
    openxlsx::writeData(workbook, sheet_name, sheet_data)
    openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)

    return(output_path)
  }

  write_single_sheet_workbook(
    sheet_name = "clean_audit",
    sheet_data = data.table::as.data.table(clean_audit_dt),
    output_path = output_paths[["clean_audit"]]
  )
  write_single_sheet_workbook(
    sheet_name = "harmonize_audit",
    sheet_data = data.table::as.data.table(harmonize_audit_dt),
    output_path = output_paths[["harmonize_audit"]]
  )
  write_single_sheet_workbook(
    sheet_name = "stage_summary",
    sheet_data = diagnostics$stage_summary,
    output_path = output_paths[["stage_summary"]]
  )
  write_single_sheet_workbook(
    sheet_name = "rule_summary",
    sheet_data = diagnostics$rule_summary,
    output_path = output_paths[["rule_summary"]]
  )
  write_single_sheet_workbook(
    sheet_name = "standardize_diagnostics",
    sheet_data = diagnostics$standardize_summary,
    output_path = output_paths[["standardize_diagnostics"]]
  )

  return(output_paths)
}
