options(
  fao.run_import_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "13-validate_log.R"), echo = FALSE)

testthat::test_that("validate_mandatory_fields_dt reports missing values deterministically", {
  input_dt <- data.table::data.table(
    product = c("wheat", ""),
    variable = c("production", "yield"),
    document = c("a.xlsx", "b.xlsx")
  )

  config <- list(column_required = c("product", "variable", "year"))

  result <- validate_mandatory_fields_dt(input_dt, config)

  testthat::expect_true(is.data.table(result$data))
  testthat::expect_true("year" %in% names(result$data))
  testthat::expect_true(length(result$errors) >= 2)
  testthat::expect_true(any(grepl("row_id '1', column 'year'", result$errors, fixed = TRUE)))
  testthat::expect_true(any(grepl("row_id '2', column 'product'", result$errors, fixed = TRUE)))
})


testthat::test_that("validate_mandatory_fields_dt returns unique messages", {
  input_dt <- data.table::data.table(
    product = c("", ""),
    variable = c("", ""),
    document = c("same.xlsx", "same.xlsx")
  )

  config <- list(column_required = c("product", "variable"))

  result <- validate_mandatory_fields_dt(input_dt, config)

  testthat::expect_identical(result$errors, unique(result$errors))
})
