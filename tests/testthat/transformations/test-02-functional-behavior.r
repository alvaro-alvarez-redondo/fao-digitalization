testthat::test_that("load_pipeline_config returns expected core fields", {
  config <- load_pipeline_config("fao_data_raw")

  testthat::expect_named(
    config,
    c(
      "project_root",
      "dataset_name",
      "paths",
      "files",
      "columns",
      "column_required",
      "column_id",
      "column_order",
      "export_config",
      "defaults",
      "messages"
    )
  )

  testthat::expect_equal(config$files$raw_data, "fao_data_raw.xlsx")
  testthat::expect_equal(config$files$wide_raw_data, "fao_data_wide_raw.xlsx")
  testthat::expect_equal(config$files$long_raw_data, "fao_data_long_raw.xlsx")

  testthat::expect_identical(config$dataset_name, "fao_data_raw")
  testthat::expect_match(config$paths$data$audit$audit_file_path, "fao_data_raw_audit\\.xlsx$")
  testthat::expect_match(config$paths$data$audit$raw_imports_mirror_dir, "raw_imports_mirror$")
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
  config <- load_pipeline_config("fao_data_raw")

  testthat::expect_identical(config$defaults$notes_value, NA_character_)
})


testthat::test_that("load_pipeline_config sets analytical target column order", {
  config <- load_pipeline_config("fao_data_raw")

  expected_order <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "value",
    "notes",
    "footnotes",
    "yearbook",
    "document"
  )

  testthat::expect_identical(config$column_order, expected_order)
})

testthat::test_that("consolidate_validated_dt reorders columns without dropping extras", {
  dt_list <- list(
    data.table::data.table(
      country = "nepal",
      continent = "asia",
      variable = "production",
      product = "rice",
      unit = "t",
      year = "2020",
      value = "1",
      notes = NA_character_,
      footnotes = "none",
      yearbook = "yb_2020",
      document = "sample_file.xlsx",
      extra_metric = "x"
    )
  )

  consolidated <- consolidate_validated_dt(dt_list, test_config)

  expected_prefix <- c(
    "continent",
    "country",
    "product",
    "variable",
    "unit",
    "year",
    "value",
    "notes",
    "footnotes",
    "yearbook",
    "document"
  )

  testthat::expect_identical(
    colnames(consolidated$data)[seq_along(expected_prefix)],
    expected_prefix
  )
  testthat::expect_true("extra_metric" %in% colnames(consolidated$data))
})


testthat::test_that("consolidate_validated_dt validates required schema coverage", {
  config_missing_required <- test_config
  config_missing_required$column_order <- setdiff(
    config_missing_required$column_order,
    "document"
  )

  testthat::expect_error(
    consolidate_validated_dt(list(data.table::data.table()), config_missing_required)
  )
})


testthat::test_that("consolidate_validated_dt fails when column_order is undefined", {
  config_missing_column_order <- test_config
  config_missing_column_order$column_order <- NULL

  testthat::expect_error(
    consolidate_validated_dt(list(data.table::data.table()), config_missing_column_order),
    regexp = "config\\$column_order"
  )
})
