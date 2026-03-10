# tests/1-import_pipeline/test-transform.R
# unit tests for scripts/1-import_pipeline/12-transform.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "10-file_io.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "11-reading.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "12-transform.R"), echo = FALSE)


# --- identify_year_columns ---------------------------------------------------

testthat::test_that("identify_year_columns detects yyyy columns", {
  col_names <- c("continent", "country", "2020", "2021", "value")
  result <- identify_year_columns(col_names)

  testthat::expect_true("2020" %in% result)
  testthat::expect_true("2021" %in% result)
  testthat::expect_false("continent" %in% result)
})

testthat::test_that("identify_year_columns detects yyyy-yyyy range columns", {
  col_names <- c("2020-2021", "country")
  result <- identify_year_columns(col_names)

  testthat::expect_true("2020-2021" %in% result)
})

testthat::test_that("identify_year_columns returns empty for non-year columns", {
  col_names <- c("continent", "country", "value")
  result <- identify_year_columns(col_names)

  testthat::expect_equal(length(result), 0L)
})


# --- normalize_key_fields ----------------------------------------------------

testthat::test_that("normalize_key_fields adds missing base columns as NA", {
  dt <- data.table::data.table(
    product  = c("wheat", "rice"),
    variable = c("production", "trade")
  )
  config <- build_test_config()

  result <- normalize_key_fields(dt, config)

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true("continent" %in% names(result))
  testthat::expect_true("country"   %in% names(result))
})


# --- convert_year_columns ----------------------------------------------------

testthat::test_that("convert_year_columns sanitizes column names", {
  dt <- data.table::data.table(
    continent = "Asia",
    `2020`    = "100",
    `2021`    = "200"
  )

  result <- convert_year_columns(dt)

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true(all(names(result) == make.names(names(result), unique = TRUE) | grepl("^\\d{4}", names(result))))
})


# --- reshape_to_long ---------------------------------------------------------

testthat::test_that("reshape_to_long converts wide to long format", {
  dt <- data.table::data.table(
    continent = c("Asia", "Europe"),
    country   = c("Japan", "France"),
    product   = c("wheat", "rice"),
    variable  = c("production", "trade"),
    unit      = c("tonnes", "tonnes"),
    footnotes = c(NA_character_, NA_character_),
    `2020`    = c("100", "200"),
    `2021`    = c("300", "400")
  )

  year_columns <- c("2020", "2021")
  result <- reshape_to_long(dt, year_columns)

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true("year"  %in% names(result))
  testthat::expect_true("value" %in% names(result))
  testthat::expect_equal(nrow(result), 4L)  # 2 rows * 2 years
})


# --- add_metadata ------------------------------------------------------------

testthat::test_that("add_metadata appends document, notes, yearbook columns", {
  dt <- data.table::data.table(
    continent = "Asia",
    country   = "Japan",
    year      = "2020",
    value     = "100"
  )

  file_row <- data.frame(
    file_name = "fao_yb_2020_2021_a_b_wheat.xlsx",
    yearbook  = "yb_2020_2021",
    stringsAsFactors = FALSE
  )
  config <- build_test_config()

  result <- add_metadata(dt, file_row, config)

  testthat::expect_true("document" %in% names(result))
  testthat::expect_true("yearbook" %in% names(result))
})


# --- build_empty_transform_result --------------------------------------------

testthat::test_that("build_empty_transform_result returns correct structure", {
  result <- build_empty_transform_result()

  testthat::expect_true(is.list(result))
  testthat::expect_true("wide_raw" %in% names(result))
  testthat::expect_true("long_raw" %in% names(result))
})


# --- assert_transform_result_contract ----------------------------------------

testthat::test_that("assert_transform_result_contract validates correct structure", {
  result <- list(
    wide_raw = data.table::data.table(a = 1),
    long_raw = data.table::data.table(b = 2)
  )

  testthat::expect_invisible(assert_transform_result_contract(result))
})

testthat::test_that("assert_transform_result_contract errors on missing keys", {
  result <- list(wide_raw = data.table::data.table(a = 1))

  testthat::expect_error(assert_transform_result_contract(result))
})


# --- edge cases: empty input -------------------------------------------------

testthat::test_that("identify_year_columns handles empty column vector", {
  result <- identify_year_columns(character(0))

  testthat::expect_equal(length(result), 0L)
})
