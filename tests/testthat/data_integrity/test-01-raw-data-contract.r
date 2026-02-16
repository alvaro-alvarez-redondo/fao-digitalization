raw_table <- data.table::data.table(
  continent = "asia",
  country = "nepal",
  product = "rice",
  variable = "production",
  unit = "t",
  year = "2020",
  value = "1",
  notes = NA_character_,
  footnotes = "none",
  yearbook = "yb_2020",
  document = "sample_file.xlsx"
)

testthat::test_that("raw data columns follow configured order", {
  expected_columns <- test_config$column_order
  testthat::expect_named(raw_table, expected_columns)
})

testthat::test_that("raw data critical values are not null", {
  critical_columns <- c("product", "variable", "year", "document")

  has_non_null_values <- raw_table |>
    dplyr::summarise(dplyr::across(
      dplyr::all_of(critical_columns),
      ~ all(!is.na(.x) & .x != "")
    )) |>
    unlist(use.names = FALSE) |>
    all()

  testthat::expect_true(has_non_null_values)
})
