source(here::here("R/config.R"), echo = FALSE)

testthat::test_that("config defines required top-level sections", {
  testthat::expect_type(config, "list")
  testthat::expect_true(all(c("input", "output", "audit", "system") %in% names(config)))
})

testthat::test_that("config audit canonical schema matches helper contract", {
  expected_columns <- c("row_index", "audit_column", "audit_type", "audit_message")

  testthat::expect_true(data.table::is.data.table(config$audit$canonical_schema))
  testthat::expect_true(all(expected_columns %in% config$audit$canonical_schema$column_name))

  if (exists("empty_audit_findings_dt", mode = "function")) {
    testthat::expect_identical(
      expected_columns,
      names(empty_audit_findings_dt())
    )
  }
})

testthat::test_that("config output column order is compatible with validator", {
  validated_order <- validate_output_column_order(list(
    column_order = config$output$column_order
  ))

  testthat::expect_identical(validated_order, config$output$column_order)
})

testthat::test_that("config system settings are defined", {
  testthat::expect_identical(config$system$excel_process_name, "EXCEL.EXE")
  testthat::expect_true(is.character(config$system$log_path))
  testthat::expect_length(config$system$log_path, 1)
})
