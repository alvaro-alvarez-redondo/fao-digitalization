testthat::test_that("discover_files returns empty tibble for empty folder", {
  temp_dir <- withr::local_tempdir()

  empty_result <- testthat::expect_warning(
    discover_files(temp_dir),
    regexp = "no \\.xlsx files found",
    ignore.case = TRUE
  )

  testthat::expect_equal(nrow(empty_result), 0)
  testthat::expect_named(empty_result, "file_path")
})

testthat::test_that("extract_file_metadata validates character inputs", {
  testthat::expect_error(
    extract_file_metadata(NULL),
    class = "rlang_error"
  )
})

testthat::test_that("discover_pipeline_files fails fast for missing import path", {
  config_missing_path <- list(paths = list(data = list(imports = list())))

  testthat::expect_error(
    discover_pipeline_files(config_missing_path),
    regexp = "config\\$paths\\$data\\$imports\\$raw"
  )
})

testthat::test_that("read_file_sheets handles missing file with error", {
  missing_path <- fs::path(withr::local_tempdir(), "missing.xlsx")

  testthat::expect_error(
    read_file_sheets(missing_path, test_config),
    regexp = "path does not exist|cannot open the connection|does not exist"
  )
})

testthat::test_that("transform_single_file returns null for empty table", {
  file_row <- tibble::tibble(
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    product = "rice"
  )

  empty_dt <- data.table::data.table()

  transformed <- transform_single_file(file_row, empty_dt, test_config)

  testthat::expect_true(is.null(transformed))
})

testthat::test_that("transform_single_file defaults missing product metadata", {
  file_row <- tibble::tibble(
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    product = NA_character_
  )

  input_dt <- data.table::data.table(
    variable = "production",
    continent = "asia",
    country = "nepal",
    unit = "t",
    footnotes = "none",
    `2020` = "1"
  )

  transformed <- testthat::expect_no_warning(
    transform_single_file(file_row, input_dt, test_config)
  )

  testthat::expect_identical(transformed$wide_raw$product[[1]], "unknown")
})

testthat::test_that("transform_single_file optionally emits missing product metadata warning", {
  file_row <- tibble::tibble(
    file_name = "sample_file.xlsx",
    yearbook = "yb_2020",
    product = NA_character_
  )

  input_dt <- data.table::data.table(
    variable = "production",
    continent = "asia",
    country = "nepal",
    unit = "t",
    footnotes = "none",
    `2020` = "1"
  )

  config_with_warning <- test_config
  config_with_warning$messages$show_missing_product_metadata_warning <- TRUE

  transformed <- testthat::expect_warning(
    transform_single_file(file_row, input_dt, config_with_warning),
    regexp = "missing product metadata",
    ignore.case = TRUE
  )

  testthat::expect_identical(transformed$wide_raw$product[[1]], "unknown")
})
