# tests/2-post_processing_pipeline/test-diagnostics.R
# unit tests for scripts/2-post_processing_pipeline/24-post_processing_diagnostics.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21-post_processing_utilities.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21b-post_processing_rule_engine.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "24-post_processing_diagnostics.R"), echo = FALSE)


# --- collect_post_processing_preflight ---------------------------------------

testthat::test_that("preflight flags invalid file naming patterns", {
  config <- build_test_config()
  dir.create(file.path(config$paths$data$audit$audit_root_dir, "templates"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(config$paths$data$audit$audit_root_dir, "post_processing_diagnostics"), recursive = TRUE, showWarnings = FALSE)

  file.create(file.path(config$paths$data$imports$cleaning, "bad_clean_name.xlsx"))
  file.create(file.path(config$paths$data$imports$harmonization, "bad_harmonize_name.xlsx"))

  result <- collect_post_processing_preflight(
    config = config,
    dataset_columns = c("unit", "value", "product")
  )

  testthat::expect_false(result$checks$cleaning_pattern_ok)
  testthat::expect_false(result$checks$harmonize_pattern_ok)
  testthat::expect_true(any(grepl("clean stage", result$issues, fixed = TRUE)))
  testthat::expect_true(any(grepl("harmonize stage", result$issues, fixed = TRUE)))
})

testthat::test_that("preflight detects column convention mismatch", {
  config <- build_test_config()
  dir.create(file.path(config$paths$data$audit$audit_root_dir, "templates"), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(config$paths$data$audit$audit_root_dir, "post_processing_diagnostics"), recursive = TRUE, showWarnings = FALSE)

  file.create(file.path(config$paths$data$imports$cleaning, "clean_rules.xlsx"))
  file.create(file.path(config$paths$data$imports$harmonization, "harmonize_rules.xlsx"))

  result <- collect_post_processing_preflight(
    config = config,
    dataset_columns = c("unit", "value", "item")
  )

  testthat::expect_false(result$passed)
  testthat::expect_true(any(grepl("missing expected columns", result$issues, fixed = TRUE)))
})

testthat::test_that("assert_post_processing_preflight aborts with stage details", {
  bad_result <- list(
    passed = FALSE,
    issues = c(
      "[clean stage] missing file",
      "[harmonize stage] missing template"
    ),
    checks = list()
  )

  testthat::expect_error(
    assert_post_processing_preflight(bad_result),
    regexp = "post-processing preflight checks failed"
  )
})


# --- summarize_stage_rules ---------------------------------------------------

testthat::test_that("summarize_stage_rules aggregates audit records", {
  audit_dt <- data.table::data.table(
    rule_file_identifier = c("clean_rules.csv", "clean_rules.csv"),
    column_source = c("product", "product"),
    column_target = c("unit", "variable"),
    affected_rows = c(5L, 3L)
  )

  result <- summarize_stage_rules(audit_dt)

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true("total_affected" %in% names(result))
  testthat::expect_true(nrow(result) >= 1L)
})


# --- build_post_processing_diagnostics ---------------------------------------

testthat::test_that("build_post_processing_diagnostics creates stage summaries", {
  audit_list <- list(
    clean = data.table::data.table(
      rule_file_identifier = "clean_rules.csv",
      column_source = "product",
      column_target = "unit",
      affected_rows = 5L
    )
  )

  result <- build_post_processing_diagnostics(audit_list)

  testthat::expect_true(is.list(result))
  testthat::expect_true("clean" %in% names(result))
})
