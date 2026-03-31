source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "12-transform.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "13-validate_log.R"), echo = FALSE)


testthat::test_that("identify_year_columns detects single and range year columns", {
  dt <- data.table::data.table(
    continent = "asia",
    country = "japan",
    `2020` = "1",
    `2020-2021` = "2",
    `x2020` = "3"
  )
  cfg <- build_test_config()

  years <- identify_year_columns(dt, cfg)

  testthat::expect_true("2020" %in% years)
  testthat::expect_true("2020-2021" %in% years)
  testthat::expect_false("x2020" %in% years)
})


testthat::test_that("normalize_key_fields normalizes expected text columns", {
  dt <- data.table::data.table(
    variable = c("Production", "Trade"),
    continent = c("Asía", "Europé"),
    country = c("Jápan", "Fránce"),
    footnotes = c("note 1/ Revised", "prel.; #2")
  )
  cfg <- build_test_config()

  out <- normalize_key_fields(dt, "Cérèals", cfg)

  testthat::expect_true(all(out$product == "cereals"))
  testthat::expect_identical(out$variable, c("production", "trade"))
  testthat::expect_identical(out$continent, c("asia", "europe"))
  testthat::expect_identical(out$country, c("japan", "france"))
  testthat::expect_true(all(!is.na(out$footnotes)))
})


testthat::test_that("reshape_to_long preserves configured id columns", {
  dt <- data.table::data.table(
    hemisphere = c("north", "south"),
    continent = c("asia", "africa"),
    country = c("japan", "kenya"),
    product = c("wheat", "rice"),
    variable = c("production", "production"),
    unit = c("tonnes", "tonnes"),
    footnotes = c(NA_character_, NA_character_),
    `2020` = c("100", "200"),
    `2021` = c("110", "210")
  )
  cfg <- build_test_config()

  out <- reshape_to_long(dt, cfg)

  testthat::expect_equal(nrow(out), 4L)
  testthat::expect_true(all(c("hemisphere", "country", "year", "value") %in% names(out)))
})


testthat::test_that("convert_year_columns stores detected year-column cache", {
  dt <- data.table::data.table(
    continent = "asia",
    country = "japan",
    `2020` = "100",
    `2021` = "110"
  )
  cfg <- build_test_config()

  out <- convert_year_columns(dt, cfg)

  testthat::expect_identical(
    attr(out, "whep_year_columns", exact = TRUE),
    c("2020", "2021")
  )
})


testthat::test_that("validate_mandatory_fields_dt uses centralized unknown document default", {
  dt <- data.table::data.table(
    continent = c("asia"),
    country = c(""),
    product = c("wheat"),
    variable = c("production")
  )
  cfg <- build_test_config()
  constants <- get_pipeline_constants()

  out <- validate_mandatory_fields_dt(dt, cfg)

  testthat::expect_identical(
    out$data$document[[1]],
    constants$defaults$unknown_document
  )
  testthat::expect_true(length(out$errors) > 0L)
})
