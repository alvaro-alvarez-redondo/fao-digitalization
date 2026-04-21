options(
  whep.run_post_processing_pipeline.auto = FALSE,
  whep.run_pipeline.auto = FALSE
)

source(
  here::here("r", "0-general_pipeline", "02-helpers.R"),
  echo = FALSE
)
source(
  here::here("r", "2-post_processing_pipeline", "24-standardize_units.R"),
  echo = FALSE
)

testthat::test_that("aggregate_standardized_rows sums duplicate groups deterministically", {
  input_dt <- data.table::data.table(
    product = c("wheat", "wheat", "rice"),
    unit = c("kg", "kg", "kg"),
    value = c(10, 20, 5)
  )

  result <- aggregate_standardized_rows(input_dt, value_column = "value")

  testthat::expect_s3_class(result, "data.table")
  testthat::expect_equal(nrow(result), 2L)
  testthat::expect_equal(result[product == "wheat", value], 30)
  testthat::expect_equal(result[product == "rice", value], 5)
})

testthat::test_that("extract_aggregated_rows returns only duplicate-group source rows", {
  input_dt <- data.table::data.table(
    product = c("wheat", "wheat", "rice"),
    unit = c("kg", "kg", "kg"),
    value = c(10, 20, 5)
  )

  result <- extract_aggregated_rows(input_dt, value_column = "value")

  testthat::expect_s3_class(result, "data.table")
  testthat::expect_equal(nrow(result), 2L)
  testthat::expect_true(all(result$product == "wheat"))
})

testthat::test_that("extract_aggregated_rows does not mutate input keys", {
  input_dt <- data.table::data.table(
    product = c("wheat", "wheat", "rice"),
    unit = c("kg", "kg", "kg"),
    value = c(10, 20, 5)
  )

  testthat::expect_null(data.table::key(input_dt))

  invisible(extract_aggregated_rows(input_dt, value_column = "value"))

  testthat::expect_null(data.table::key(input_dt))
})
