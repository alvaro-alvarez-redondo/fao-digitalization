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

#' @title Build post-processing rule summaries
#' @description Creates stage-specific rule summaries for clean and harmonize
#' audit tables.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_diagnostics Named diagnostics list for standardize layer.
#' retained for backward compatibility.
#' @return Named list with `clean_rule_summary` and `harmonize_rule_summary`
#' data.tables.
#' @importFrom checkmate assert_data_frame assert_list
build_post_processing_diagnostics <- function(
  clean_audit_dt,
  harmonize_audit_dt,
  standardize_diagnostics = list()
) {
  checkmate::assert_data_frame(clean_audit_dt, min.rows = 0)
  checkmate::assert_data_frame(harmonize_audit_dt, min.rows = 0)
  checkmate::assert_list(standardize_diagnostics)

  summarize_stage_rules <- function(audit_dt, stage_name) {
    stage_audit_dt <- data.table::as.data.table(audit_dt)

    required_columns <- c(
      "rule_file_identifier",
      "column_source",
      "value_source_raw",
      "column_target",
      "value_target_raw",
      "value_target_clean",
      "affected_rows"
    )

    missing_columns <- setdiff(required_columns, names(stage_audit_dt))
    if (length(missing_columns) > 0L) {
      for (column_name in missing_columns) {
        stage_audit_dt[, (column_name) := NA_character_]
      }
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
        value_target_clean = character(),
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
        value_target_clean
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
        value_target_clean,
        affected_rows
      )
    ])
  }

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

#' @title Persist post-processing audit workbooks
#' @description Writes deterministic Excel outputs under
#' `audit_root_dir/post_processing_diagnostics` using a single rule-summary
#' workbook with one sheet per stage.
#' @param clean_audit_dt Clean-stage audit table.
#' @param harmonize_audit_dt Harmonize-stage audit table.
#' @param standardize_diagnostics Named diagnostics list for standardize layer.
#' retained for backward compatibility.
#' @param dataset_name Character scalar dataset name.
#' @param execution_timestamp_utc Character scalar run timestamp (retained for backward compatibility).
#' @param config Named configuration list.
#' @return Named character vector containing `rule_summary` workbook path.
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

  run_token <- gsub("[^0-9A-Za-z]", "", execution_timestamp_utc)

  output_paths <- c(
    rule_summary = fs::path(
      diagnostics_dir,
      paste0("post_processing_audit_rule_summary_", run_token, ".xlsx")
    )
  )

  add_traceability_columns <- function(sheet_dt) {
    traced_dt <- data.table::as.data.table(sheet_dt)
    traced_dt[, dataset_name := dataset_name]
    traced_dt[, execution_timestamp_utc := execution_timestamp_utc]

    data.table::setcolorder(
      traced_dt,
      c(
        "dataset_name",
        "execution_timestamp_utc",
        setdiff(
          colnames(traced_dt),
          c("dataset_name", "execution_timestamp_utc")
        )
      )
    )

    return(traced_dt)
  }

  workbook <- openxlsx::createWorkbook()

  openxlsx::addWorksheet(workbook, "clean")
  openxlsx::writeData(
    workbook,
    "clean",
    add_traceability_columns(diagnostics$clean_rule_summary)
  )

  openxlsx::addWorksheet(workbook, "harmonize")
  openxlsx::writeData(
    workbook,
    "harmonize",
    add_traceability_columns(diagnostics$harmonize_rule_summary)
  )

  openxlsx::saveWorkbook(
    workbook,
    output_paths[["rule_summary"]],
    overwrite = TRUE
  )

  return(output_paths)
}
