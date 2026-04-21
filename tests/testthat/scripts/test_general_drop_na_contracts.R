options(
  whep.run_general_pipeline.auto = FALSE,
  whep.run_pipeline.auto = FALSE
)

source(
  here::here("r", "0-general_pipeline", "01-setup.R"),
  echo = FALSE
)
source(
  here::here("r", "0-general_pipeline", "02-helpers.R"),
  echo = FALSE
)

testthat::test_that("drop_na_value_rows removes NA rows for data.table input", {
  input_dt <- data.table::data.table(
    country = c("japan", "france", "italy"),
    value = c("100", NA_character_, "300")
  )

  result <- withr::with_options(list(whep.drop_na_values = TRUE), {
    drop_na_value_rows(input_dt)
  })

  testthat::expect_s3_class(result, "data.table")
  testthat::expect_equal(nrow(result), 2L)
  testthat::expect_true(all(!is.na(result$value)))
})

testthat::test_that("drop_na_value_rows keeps rows when option is disabled", {
  input_dt <- data.table::data.table(
    country = c("japan", "france"),
    value = c("100", NA_character_)
  )

  result <- withr::with_options(list(whep.drop_na_values = FALSE), {
    drop_na_value_rows(input_dt)
  })

  testthat::expect_equal(nrow(result), 2L)
})

testthat::test_that("drop_na_value_rows supports data.frame input", {
  input_df <- data.frame(
    country = c("japan", "france", "italy"),
    value = c("100", NA_character_, "300"),
    stringsAsFactors = FALSE
  )

  result <- drop_na_value_rows(input_df)

  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_equal(nrow(result), 2L)
  testthat::expect_true(all(!is.na(result$value)))
})
