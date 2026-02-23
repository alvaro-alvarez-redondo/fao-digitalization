source(here::here("R/1-import_pipeline/11-reading.R"), echo = FALSE)

testthat::test_that("assert_read_result_contract accepts stable read result schema", {
  read_result <- list(
    data = data.frame(value = "1", stringsAsFactors = FALSE),
    errors = character(0)
  )

  output <- assert_read_result_contract(read_result)

  testthat::expect_true(isTRUE(output))
})

testthat::test_that("assert_read_result_contract rejects missing errors field", {
  bad_result <- list(data = data.frame(value = "1", stringsAsFactors = FALSE))

  testthat::expect_error(
    assert_read_result_contract(bad_result),
    class = "rlang_error"
  )
})

testthat::test_that("normalize_pipeline_read_result returns empty standardized payload on failed read", {
  read_result <- list(
    result = NULL,
    errors = "failed read"
  )

  output <- normalize_pipeline_read_result(read_result)

  testthat::expect_true(data.table::is.data.table(output$data))
  testthat::expect_equal(nrow(output$data), 0)
  testthat::expect_identical(output$errors, "failed read")
})

testthat::test_that("normalize_pipeline_read_result preserves successful data and merges errors", {
  read_result <- list(
    result = list(
      data = data.frame(value = "2", stringsAsFactors = FALSE),
      errors = "sheet warning"
    ),
    errors = "file warning"
  )

  output <- normalize_pipeline_read_result(read_result)

  testthat::expect_true(data.table::is.data.table(output$data))
  testthat::expect_identical(names(output$data), "value")
  testthat::expect_identical(output$errors, c("file warning", "sheet warning"))
})
