# tests/1-import_pipeline/test-output.R
# unit tests for scripts/1-import_pipeline/15-output.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "10-file_io.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "11-reading.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "12-transform.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "13-validate_log.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "15-output.R"), echo = FALSE)


# --- validate_output_column_order --------------------------------------------

testthat::test_that("validate_output_column_order returns TRUE for valid config", {
  config <- build_test_config()

  result <- validate_output_column_order(config)

  testthat::expect_true(result)
})

testthat::test_that("validate_output_column_order fails when column_order is missing", {
  config <- list()

  testthat::expect_error(validate_output_column_order(config))
})


# --- consolidate_audited_dt --------------------------------------------------

testthat::test_that("consolidate_audited_dt combines multiple data.tables", {
  dt1 <- build_sample_long_dt(2L)
  dt2 <- build_sample_long_dt(2L)

  config <- build_test_config()

  result <- consolidate_audited_dt(list(dt1, dt2), config)

  testthat::expect_true(is.list(result))
  testthat::expect_true("data"     %in% names(result))
  testthat::expect_true("warnings" %in% names(result))
  testthat::expect_equal(nrow(result$data), 4L)
})

testthat::test_that("consolidate_audited_dt handles empty list", {
  config <- build_test_config()

  result <- consolidate_audited_dt(list(), config)

  testthat::expect_true(is.list(result))
  testthat::expect_equal(nrow(result$data), 0L)
})

testthat::test_that("consolidate_audited_dt enforces column order from config", {
  dt <- build_sample_long_dt()
  config <- build_test_config()

  result <- consolidate_audited_dt(list(dt), config)

  # columns should follow config$column_order where present
  output_cols <- names(result$data)
  expected_order <- intersect(config$column_order, output_cols)
  actual_order   <- output_cols[output_cols %in% config$column_order]

  testthat::expect_identical(actual_order, expected_order)
})


# --- hemisphere in target_schema ---------------------------------------------

testthat::test_that("validate_output_column_order requires hemisphere in column_order", {
  config <- build_test_config()

  testthat::expect_true("hemisphere" %in% config$column_order)
})

testthat::test_that("validate_output_column_order errors when config column_order omits hemisphere", {
  config        <- build_test_config()
  config$column_order <- setdiff(config$column_order, "hemisphere")

  testthat::expect_error(validate_output_column_order(config))
})
