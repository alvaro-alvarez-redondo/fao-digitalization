testthat::test_that("load_pipeline_config returns expected core fields", {
  config <- load_pipeline_config()

  testthat::expect_named(
    config,
    c(
      "project_root",
      "paths",
      "files",
      "columns",
      "column_required",
      "column_id",
      "column_order",
      "export_config",
      "defaults"
    )
  )

  testthat::expect_equal(config$files$raw_data, "fao_data_raw.xlsx")
  testthat::expect_equal(config$files$wide_raw_data, "fao_data_wide_raw.xlsx")
  testthat::expect_equal(config$files$long_raw_data, "fao_data_long_raw.xlsx")
})

testthat::test_that("transform_file_dt returns expected class and dimensions", {
  input_dt <- data.table::data.table(
    variable = "production",
    continent = "asia",
    country = "nepal",
    unit = "t",
    footnotes = "none",
    `2020` = "1",
    `2021` = "2"
  )

  transformed <- transform_file_dt(
    df = input_dt,
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    product_name = "rice",
    config = test_config
  )

  testthat::expect_true(data.table::is.data.table(transformed$wide_raw))
  testthat::expect_true(data.table::is.data.table(transformed$long_raw))
  testthat::expect_equal(nrow(transformed$long_raw), 2)
  testthat::expect_equal(ncol(transformed$long_raw), length(test_config$column_order))
})

testthat::test_that("add_metadata preserves na notes defaults", {
  input_df <- data.frame(country = "nepal", year = "2020", value = "1")
  config_with_na <- list(defaults = list(notes_value = NA_character_))

  output_dt <- add_metadata(
    fao_data_long_raw = input_df,
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    config = config_with_na
  )

  testthat::expect_true(data.table::is.data.table(output_dt))
  testthat::expect_identical(output_dt$notes, NA_character_)
})

testthat::test_that("load_pipeline_config sets missing notes default", {
  config <- load_pipeline_config()

  testthat::expect_identical(config$defaults$notes_value, NA_character_)
})
