# script: post-processing diagnostic checks
# description: preflight checks to identify likely failures before post-processing runs.

#' @title collect post-processing preflight checks
#' @description run deterministic preflight checks for file layout, rule naming,
#' and column compatibility prior to post-processing execution.
#' @param config named configuration list.
#' @param dataset_columns character vector of available input column names.
#' @param expected_columns character vector of length three defining the active
#' `(unit, value, product)` convention expected by the current runner call.
#' @return named list with `passed`, `issues`, and `checks`.
#' @importFrom checkmate assert_list assert_character
#' @importFrom here here
#' @importFrom fs dir_exists dir_ls path
#' @examples
#' \dontrun{
#' collect_post_processing_preflight(
#'   config = config,
#'   dataset_columns = c("unit", "value", "product")
#' )
#' }
collect_post_processing_preflight <- function(
  config,
  dataset_columns,
  expected_columns = c("unit", "value", "product")
) {
  checkmate::assert_list(config, min.len = 1)
  checkmate::assert_character(dataset_columns, any.missing = FALSE)
  checkmate::assert_character(expected_columns, len = 3, any.missing = FALSE)

  issues <- character(0)
  checks <- list()

  cleaning_dir <- config$paths$data$imports$cleaning
  harmonization_dir <- config$paths$data$imports$harmonization
  template_dir <- here::here("data", "exports", "templates")

  checks$cleaning_dir_exists <- fs::dir_exists(cleaning_dir)
  checks$harmonization_dir_exists <- fs::dir_exists(harmonization_dir)

  if (!checks$cleaning_dir_exists) {
    issues <- c(
      issues,
      "[cleaning stage] cleaning imports directory is missing: config$paths$data$imports$cleaning"
    )
  }

  if (!checks$harmonization_dir_exists) {
    issues <- c(
      issues,
      "[harmonization stage] harmonization imports directory is missing: config$paths$data$imports$harmonization"
    )
  }

  cleaning_files <- if (checks$cleaning_dir_exists) {
    fs::dir_ls(cleaning_dir, regexp = "\\.xlsx$", type = "file")
  } else {
    character(0)
  }

  harmonization_files <- if (checks$harmonization_dir_exists) {
    fs::dir_ls(harmonization_dir, regexp = "\\.xlsx$", type = "file")
  } else {
    character(0)
  }

  checks$cleaning_pattern_ok <- all(
    basename(cleaning_files) == "cleaning_template.xlsx" |
      grepl("^cleaning_.*\\.xlsx$", basename(cleaning_files))
  )

  checks$harmonization_pattern_ok <- all(
    basename(harmonization_files) == "harmonization_template.xlsx" |
      grepl("^harmonization_.*\\.xlsx$", basename(harmonization_files))
  )

  if (!checks$cleaning_pattern_ok) {
    issues <- c(
      issues,
      "[cleaning stage] found .xlsx files that do not match cleaning_*.xlsx naming"
    )
  }

  if (!checks$harmonization_pattern_ok) {
    issues <- c(
      issues,
      "[harmonization stage] found .xlsx files that do not match harmonization_*.xlsx naming"
    )
  }

  numeric_template_path <- fs::path(
    template_dir,
    "numeric_harmonization_template.xlsx"
  )
  checks$numeric_template_present <- file.exists(numeric_template_path)

  if (!checks$numeric_template_present) {
    issues <- c(
      issues,
      "[numeric harmonization stage] numeric_harmonization_template.xlsx not found in data/exports/templates"
    )
  }

  legacy_columns <- c("unit", "value", "product")
  auto_columns <- c("unit_name", "quantity", "item")

  has_legacy <- all(legacy_columns %in% dataset_columns)
  has_auto <- all(auto_columns %in% dataset_columns)
  has_expected <- all(expected_columns %in% dataset_columns)

  checks$has_legacy_columns <- has_legacy
  checks$has_auto_columns <- has_auto
  checks$has_expected_columns <- has_expected

  if (!has_expected) {
    issues <- c(
      issues,
      paste0(
        "[run_post_processing_pipeline] required columns missing for this run: ",
        paste(expected_columns, collapse = ", ")
      )
    )
  }

  if (!has_legacy && has_auto && identical(expected_columns, legacy_columns)) {
    issues <- c(
      issues,
      "[run_post_processing_pipeline_batch] input appears to use (unit_name, quantity, item) while batch defaults expect (unit, value, product)"
    )
  }

  if (has_legacy && !has_auto && identical(expected_columns, auto_columns)) {
    issues <- c(
      issues,
      "[run_post_processing_pipeline_auto] input appears to use (unit, value, product) while auto-run expects (unit_name, quantity, item)"
    )
  }

  return(list(
    passed = length(issues) == 0,
    issues = issues,
    checks = checks
  ))
}

#' @title assert post-processing preflight
#' @description abort with stage-specific diagnostics when preflight checks fail.
#' @param preflight_result list returned by `collect_post_processing_preflight()`.
#' @return invisible TRUE when all checks pass.
#' @importFrom checkmate assert_list assert_flag assert_character
#' @importFrom cli cli_abort
#' @examples
#' \dontrun{
#' result <- collect_post_processing_preflight(config, c("unit", "value", "product"))
#' assert_post_processing_preflight(result)
#' }
assert_post_processing_preflight <- function(preflight_result) {
  checkmate::assert_list(preflight_result, min.len = 1)
  checkmate::assert_flag(preflight_result$passed)
  checkmate::assert_character(preflight_result$issues, any.missing = FALSE)

  if (!isTRUE(preflight_result$passed)) {
    cli::cli_abort(c(
      "post-processing preflight checks failed",
      preflight_result$issues
    ))
  }

  return(invisible(TRUE))
}
