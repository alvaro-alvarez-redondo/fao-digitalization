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
