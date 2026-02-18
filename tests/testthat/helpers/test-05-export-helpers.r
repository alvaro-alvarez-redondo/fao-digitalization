source(here::here("R/3-export_pipeline/32-export_lists.R"), echo = FALSE)

testthat::test_that("get_unique_column returns sorted unique values", {
  sample_df <- data.frame(country = c("brazil", "argentina", "brazil"))

  unique_country <- get_unique_column(sample_df, "country")

  testthat::expect_identical(unique_country, c("argentina", "brazil"))
})

testthat::test_that("get_unique_column aborts when requested column is not present", {
  sample_df <- data.frame(country = c("brazil", "argentina"))

  testthat::expect_error(
    get_unique_column(sample_df, "product"),
    "must be one of"
  )
})

testthat::test_that("normalize_sheet_name is resilient to missing and empty labels", {
  normalized <- normalize_sheet_name(c(NA_character_, "***", "country"))

  testthat::expect_identical(normalized, c("unknown", "unknown", "country"))
})

testthat::test_that("normalize_sheet_name truncates labels to excel length limit", {
  long_name <- "abcdefghijklmnopqrstuvwxyz_123456"

  normalized <- normalize_sheet_name(long_name)

  testthat::expect_identical(nchar(normalized), 31L)
})

testthat::test_that("export function signatures remain backward compatible", {
  testthat::expect_identical(
    names(formals(export_single_column_list)),
    c("df", "col_name", "config", "overwrite")
  )
  testthat::expect_identical(
    names(formals(export_selected_unique_lists)),
    c("df", "config", "overwrite")
  )
  testthat::expect_identical(formals(export_single_column_list)$overwrite, TRUE)
  testthat::expect_identical(formals(export_selected_unique_lists)$overwrite, TRUE)
})
