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
  testthat::expect_match(
    config$paths$data$audit$audit_file_path,
    "fao_data_raw_audit\\.xlsx$"
  )
  testthat::expect_match(
    config$paths$data$audit$raw_imports_mirror_dir,
    "raw_imports_mirror$"
  )
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
  testthat::expect_equal(
    ncol(transformed$long_raw),
    length(test_config$column_order)
  )
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

testthat::test_that("consolidate_audited_dt reorders columns without dropping extras", {
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

  consolidated <- consolidate_audited_dt(dt_list, test_config)

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


testthat::test_that("consolidate_audited_dt validates required schema coverage", {
  config_missing_required <- test_config
  config_missing_required$column_order <- setdiff(
    config_missing_required$column_order,
    "document"
  )

  testthat::expect_error(
    consolidate_audited_dt(
      list(data.table::data.table()),
      config_missing_required
    )
  )
})


testthat::test_that("consolidate_audited_dt fails when column_order is undefined", {
  config_missing_column_order <- test_config
  config_missing_column_order$column_order <- NULL

  testthat::expect_error(
    consolidate_audited_dt(
      list(data.table::data.table()),
      config_missing_column_order
    ),
    regexp = "config\\$column_order"
  )
})


testthat::test_that("load_pipeline_config builds dataset-scoped audit paths", {
  config <- load_pipeline_config("my dataset")

  testthat::expect_identical(config$dataset_name, "my_dataset")
  testthat::expect_match(
    config$paths$data$audit$audit_dir,
    "data[/\\]audit[/\\]my_dataset$"
  )
  testthat::expect_match(
    config$paths$data$audit$audit_file_path,
    "my_dataset_audit\\.xlsx$"
  )
  testthat::expect_match(
    config$paths$data$audit$raw_imports_mirror_dir,
    "data[/\\]audit[/\\]my_dataset[/\\]raw_imports_mirror$"
  )
})

testthat::test_that("load_pipeline_config can infer dataset name from data attribute", {
  sample_dt <- data.table::data.table(id = 1)
  attr(sample_dt, "dataset_name") <- "survey results"

  config <- load_pipeline_config(dataset_name = "", data = sample_dt)

  testthat::expect_identical(config$dataset_name, "survey_results")
  testthat::expect_match(
    config$paths$data$audit$audit_file_path,
    "survey_results_audit\\.xlsx$"
  )
})

testthat::test_that("create_required_directories normalizes file targets to parent folders", {
  temp_root <- withr::local_tempdir()

  paths <- list(
    data = list(
      audit = list(
        audit_dir = fs::path(temp_root, "audit", "dataset_a"),
        audit_file_path = fs::path(
          temp_root,
          "audit",
          "dataset_a",
          "dataset_a_audit.xlsx"
        )
      )
    )
  )

  created_dirs <- create_required_directories(paths)

  testthat::expect_true(fs::dir_exists(fs::path(
    temp_root,
    "audit",
    "dataset_a"
  )))
  testthat::expect_false(fs::dir_exists(fs::path(
    temp_root,
    "audit",
    "dataset_a",
    "dataset_a_audit.xlsx"
  )))
  testthat::expect_true(
    fs::path(temp_root, "audit", "dataset_a") %in% created_dirs
  )
})

testthat::test_that("resolve_product_name applies unknown fallback for missing product", {
  file_row <- data.frame(
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    product = NA_character_
  )

  resolved_product <- resolve_product_name(file_row, test_config)

  testthat::expect_identical(resolved_product, "unknown")
})

testthat::test_that("resolve_product_name trims whitespace from product names", {
  file_row <- data.frame(
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    product = "  rice  "
  )

  resolved_product <- resolve_product_name(file_row, test_config)

  testthat::expect_identical(resolved_product, "rice")
})
