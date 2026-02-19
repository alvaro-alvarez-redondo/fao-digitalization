testthat::test_that("identify_year_columns detects year and year-range names", {
  df <- data.frame(country = "x", `2020` = 1, `2020-2021` = 2, check.names = FALSE)
  config <- list(column_order = c("country", "year", "value"))

  output <- identify_year_columns(df, config)

  testthat::expect_identical(output, c("2020", "2020-2021"))
})

testthat::test_that("reshape_to_long aborts with cli error when no year columns exist", {
  df <- data.frame(country = "x")
  config <- list(column_id = "country", column_order = c("country", "year", "value"))

  testthat::expect_error(
    reshape_to_long(df, config),
    class = "rlang_error"
  )
})

testthat::test_that("transform_single_file preserves null return for empty data", {
  file_row <- data.frame(
    file_name = "fao_file.xlsx",
    yearbook = "yb_2020_2021",
    product = "rice"
  )

  config <- list(
    column_required = c("product", "variable", "continent", "country"),
    column_id = c("product", "variable", "continent", "country"),
    column_order = c(
      "product",
      "variable",
      "continent",
      "country",
      "year",
      "value"
    ),
    defaults = list(notes_value = NA_character_),
    messages = list(show_missing_product_metadata_warning = FALSE)
  )

  output <- transform_single_file(file_row, data.frame(), config)

  testthat::expect_null(output)
})

testthat::test_that("process_files keeps backward-compatible length validation", {
  file_list_dt <- data.frame(
    file_name = c("a.xlsx", "b.xlsx"),
    yearbook = c("yb_1", "yb_2"),
    product = c("rice", "wheat")
  )

  config <- list()

  testthat::expect_error(
    process_files(file_list_dt, read_data_list = list(data.frame()), config = config),
    "length must match"
  )
})
