# script: post-processing diagnostics
# description: consume stage audits and persist deterministic audit artifacts
# under centralized audit root directory.

#' @title Collect post-processing preflight
#' @description Validates stage import directories and expected input columns.
#' @param config Named configuration list.
#' @param dataset_columns Character vector dataset columns.
#' @return List with `passed`, `issues`, and `checks`.
collect_post_processing_preflight <- function(
  config,
  dataset_columns,
  expected_columns = dataset_columns
) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_character(dataset_columns, any.missing = FALSE)
  checkmate::assert_character(expected_columns, any.missing = FALSE, min.len = 1)

  clean_dir <- config$paths$data$imports$cleaning
  harmonize_dir <- config$paths$data$imports$harmonization

  checkmate::assert_string(clean_dir, min.chars = 1)
  checkmate::assert_string(harmonize_dir, min.chars = 1)

  checks <- list(
    clean_import_dir_exists = fs::dir_exists(clean_dir),
    harmonize_import_dir_exists = fs::dir_exists(harmonize_dir)
  )

  issues <- character(0)

  if (!checks$clean_import_dir_exists) {
    issues <- c(issues, "[clean] missing cleaning imports directory")
  }

  if (!checks$harmonize_import_dir_exists) {
    issues <- c(issues, "[harmonize] missing harmonization imports directory")
  }


  has_expected_columns <- all(expected_columns %in% dataset_columns)
  checks$has_expected_columns <- has_expected_columns

  if (!has_expected_columns) {
    issues <- c(
      issues,
      paste0(
        "[post-processing] missing expected dataset columns: ",
        paste(setdiff(expected_columns, dataset_columns), collapse = ", ")
      )
    )
  }

  return(list(passed = length(issues) == 0, issues = issues, checks = checks))
}

#' @title Assert post-processing preflight
#' @description Aborts when preflight contains issues.
#' @param preflight_result List returned by `collect_post_processing_preflight()`.
#' @return Invisibly returns `TRUE`.
assert_post_processing_preflight <- function(preflight_result) {
  checkmate::assert_list(preflight_result, min.len = 1)
  checkmate::assert_flag(preflight_result$passed)
  checkmate::assert_character(preflight_result$issues, any.missing = FALSE)

  if (!isTRUE(preflight_result$passed)) {
    cli::cli_abort(c("Post-processing preflight checks failed", preflight_result$issues))
  }

  return(invisible(TRUE))
}

#' @title Build stage audit summary
#' @description Aggregates rows and rules for one stage audit table.
#' @param stage_audit_dt Stage audit table.
#' @return One-row summary data table.
build_stage_audit_summary <- function(stage_audit_dt) {
  checkmate::assert_data_frame(stage_audit_dt, min.rows = 0)

  audit_dt <- data.table::as.data.table(stage_audit_dt)

  if (nrow(audit_dt) == 0) {
    return(data.table::data.table(
      execution_stage = character(0),
      matched_rows = integer(0),
      matched_rules = integer(0),
      rule_files = integer(0)
    ))
  }

  summary_dt <- audit_dt[
    ,
    .(
      matched_rows = as.integer(sum(affected_rows)),
      matched_rules = uniqueN(paste(column_source, value_source_raw, column_target, value_target_raw, sep = "|")),
      rule_files = uniqueN(rule_file_identifier)
    ),
    by = .(execution_stage)
  ][order(execution_stage)]

  return(summary_dt)
}

#' @title Persist stage audit workbook
#' @description Writes one deterministic workbook for stage audit and summary.
#' @param stage_audit_dt Stage audit table.
#' @param config Named configuration list.
#' @param stage Character scalar stage name.
#' @param dataset_name Character scalar dataset name.
#' @param execution_timestamp_utc Character scalar timestamp.
#' @return Character scalar output path.
persist_stage_audit_workbook <- function(
  stage_audit_dt,
  config,
  stage,
  dataset_name,
  execution_timestamp_utc
) {
  checkmate::assert_data_frame(stage_audit_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(stage, min.chars = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  audit_paths <- get_post_processing_audit_paths(config)
  post_processing_dir <- audit_paths$post_processing_dir

  file_stamp <- gsub("[-:]", "", execution_timestamp_utc)
  file_stamp <- gsub("T|Z", "_", file_stamp)

  output_path <- file.path(
    post_processing_dir,
    paste0(stage, "_audit_", dataset_name, "_", file_stamp, ".xlsx")
  )

  workbook <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(workbook, "rule_audit")
  openxlsx::writeData(workbook, "rule_audit", data.table::as.data.table(stage_audit_dt))

  openxlsx::addWorksheet(workbook, "summary")
  openxlsx::writeData(workbook, "summary", build_stage_audit_summary(stage_audit_dt))

  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)

  return(output_path)
}

#' @title Persist post-processing diagnostics workbook
#' @description Writes combined diagnostics workbook under centralized diagnostics directory.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param config Named configuration list.
#' @param dataset_name Character scalar dataset name.
#' @param execution_timestamp_utc Character scalar timestamp.
#' @return Character scalar output path.
persist_post_processing_diagnostics <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  config,
  dataset_name,
  execution_timestamp_utc
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_string(dataset_name, min.chars = 1)
  checkmate::assert_string(execution_timestamp_utc, min.chars = 1)

  audit_paths <- get_post_processing_audit_paths(config)
  diagnostics_dir <- audit_paths$diagnostics_dir

  file_stamp <- gsub("[-:]", "", execution_timestamp_utc)
  file_stamp <- gsub("T|Z", "_", file_stamp)

  output_path <- file.path(
    diagnostics_dir,
    paste0("post_processing_diagnostics_", dataset_name, "_", file_stamp, ".xlsx")
  )

  workbook <- openxlsx::createWorkbook()

  openxlsx::addWorksheet(workbook, "clean_summary")
  openxlsx::writeData(workbook, "clean_summary", build_stage_audit_summary(clean_audit_dt))

  openxlsx::addWorksheet(workbook, "harmonize_summary")
  openxlsx::writeData(workbook, "harmonize_summary", build_stage_audit_summary(harmonize_audit_dt))

  openxlsx::saveWorkbook(workbook, output_path, overwrite = TRUE)

  return(output_path)
}
