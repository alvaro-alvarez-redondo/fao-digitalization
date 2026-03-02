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
    issues <- c(issues, "[clean stage] missing cleaning imports directory")
  }

  if (!checks$harmonize_dir_exists) {
    issues <- c(issues, "[harmonize stage] missing harmonization imports directory")
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

  checks$cleaning_pattern_ok <- all(grepl("^cleaning_.*\\.(xlsx|xls|csv)$", basename(cleaning_files)))
  checks$harmonize_pattern_ok <- all(grepl("^(harmonize|harmonization)_.*\\.(xlsx|xls|csv)$", basename(harmonization_files)))

  if (!checks$cleaning_pattern_ok) {
    issues <- c(issues, "[clean stage] invalid cleaning file naming pattern")
  }

  if (!checks$harmonize_pattern_ok) {
    issues <- c(issues, "[harmonize stage] invalid harmonize file naming pattern")
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
#' @description Creates stage and rule-level summaries from clean and harmonize
#' audit metadata.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @return Named list with `stage_summary` and `rule_summary` data.tables.
#' @importFrom checkmate assert_data_frame
build_post_processing_diagnostics <- function(clean_audit_dt, harmonize_audit_dt) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)

  combined_audit <- data.table::rbindlist(
    list(
      data.table::as.data.table(clean_audit_dt),
      data.table::as.data.table(harmonize_audit_dt)
    ),
    use.names = TRUE,
    fill = TRUE
  )

  if (nrow(combined_audit) == 0) {
    return(list(
      stage_summary = data.table::data.table(
        execution_stage = character(),
        matched_rows = integer(),
        matched_rules = integer(),
        rule_files = integer()
      ),
      rule_summary = data.table::data.table()
    ))
  }

  stage_summary <- combined_audit[
    ,
    .(
      matched_rows = as.integer(sum(affected_rows)),
      matched_rules = uniqueN(paste(column_source, value_source_raw, column_target, value_target_raw, sep = "|")),
      rule_files = uniqueN(rule_file_identifier)
    ),
    by = .(execution_stage)
  ][order(execution_stage)]

  rule_summary <- combined_audit[
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

  return(list(stage_summary = stage_summary, rule_summary = rule_summary))
}

#' @title Persist post-processing audit workbook
#' @description Writes deterministic Excel output under `audit_root_dir/diagnostics`.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param dataset_name Character scalar dataset name.
#' @param execution_timestamp_utc Character scalar run timestamp.
#' @param config Named configuration list.
#' @return Character scalar path of written workbook.
#' @importFrom checkmate assert_data_frame assert_string assert_list
#' @importFrom fs dir_create path
persist_post_processing_audit <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  dataset_name,
  execution_timestamp_utc,
  config
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)
  checkmate::assert_list(config, min.len = 1)

  diagnostics <- build_post_processing_diagnostics(clean_audit_dt, harmonize_audit_dt)

  audit_paths <- initialize_post_processing_audit_root(config)
  diagnostics_dir <- audit_paths$diagnostics_dir
  fs::dir_create(diagnostics_dir, recurse = TRUE)

  file_stamp <- gsub("[-:]", "", execution_timestamp_utc)
  file_stamp <- gsub("T|Z", "_", file_stamp)

  output_path <- fs::path(
    diagnostics_dir,
    paste0("post_processing_audit_", dataset_name, "_", file_stamp, ".xlsx")
  )

  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "clean_audit")
  openxlsx::writeData(workbook, "clean_audit", data.table::as.data.table(clean_audit_dt))

  openxlsx::addWorksheet(workbook, "harmonize_audit")
  openxlsx::writeData(workbook, "harmonize_audit", data.table::as.data.table(harmonize_audit_dt))

  openxlsx::addWorksheet(workbook, "stage_summary")
  openxlsx::writeData(workbook, "stage_summary", diagnostics$stage_summary)

  openxlsx::addWorksheet(workbook, "rule_summary")
  openxlsx::writeData(workbook, "rule_summary", diagnostics$rule_summary)

  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)

  return(output_path)
}
