# tests/1-import_pipeline/test-validate-log.R
# unit tests for scripts/1-import_pipeline/13-validate_log.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "10-file_io.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "11-reading.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "12-transform.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "13-validate_log.R"), echo = FALSE)


# --- validate_mandatory_fields_dt --------------------------------------------

testthat::test_that("validate_mandatory_fields_dt returns no errors for complete data", {
  dt <- build_sample_long_dt()
  config <- build_test_config()

  result <- validate_mandatory_fields_dt(dt, config)

  testthat::expect_true(is.list(result))
  testthat::expect_true("data" %in% names(result))
  testthat::expect_true("errors" %in% names(result))
  testthat::expect_equal(length(result$errors), 0L)
})

testthat::test_that("validate_mandatory_fields_dt creates missing columns", {
  dt <- data.table::data.table(
    product  = "wheat",
    variable = "production",
    unit     = "tonnes",
    year     = "2020",
    value    = "100",
    document = "test.xlsx"
  )
  config <- build_test_config()

  result <- validate_mandatory_fields_dt(dt, config)

  # Missing columns (continent, country) should be created as NA
  testthat::expect_true("continent" %in% names(result$data))
  testthat::expect_true("country"   %in% names(result$data))
})


# --- detect_duplicates_dt ---------------------------------------------------

testthat::test_that("detect_duplicates_dt finds duplicate rows", {
  dt <- data.table::data.table(
    product  = c("wheat", "wheat"),
    variable = c("production", "production"),
    year     = c("2020", "2020"),
    value    = c("100", "100"),
    document = c("file1.xlsx", "file1.xlsx"),
    continent = c("Asia", "Asia"),
    country   = c("Japan", "Japan"),
    unit      = c("tonnes", "tonnes"),
    footnotes = c(NA_character_, NA_character_),
    yearbook  = c("yb_2024", "yb_2024"),
    notes     = c(NA_character_, NA_character_)
  )

  result <- detect_duplicates_dt(dt)

  testthat::expect_true(is.list(result))
  testthat::expect_true("data" %in% names(result))
})

testthat::test_that("detect_duplicates_dt returns clean result for unique rows", {
  dt <- build_sample_long_dt()

  result <- detect_duplicates_dt(dt)

  testthat::expect_true(is.list(result))
})


# --- validate_long_dt --------------------------------------------------------

testthat::test_that("validate_long_dt runs full validation", {
  dt <- build_sample_long_dt()
  config <- build_test_config()

  result <- validate_long_dt(dt, config)

  testthat::expect_true(is.list(result))
  testthat::expect_true("data"   %in% names(result))
  testthat::expect_true("errors" %in% names(result))
})


# --- edge cases: empty input -------------------------------------------------

testthat::test_that("validate_long_dt handles empty data.table", {
  dt <- data.table::data.table(
    continent = character(0),
    country   = character(0),
    product   = character(0),
    variable  = character(0),
    unit      = character(0),
    year      = character(0),
    value     = character(0),
    notes     = character(0),
    footnotes = character(0),
    yearbook  = character(0),
    document  = character(0)
  )
  config <- build_test_config()

  result <- validate_long_dt(dt, config)

  testthat::expect_true(is.list(result))
  testthat::expect_equal(nrow(result$data), 0L)
})
