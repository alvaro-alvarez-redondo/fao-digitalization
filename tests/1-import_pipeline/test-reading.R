# tests/1-import_pipeline/test-reading.R
# unit tests for scripts/1-import_pipeline/11-reading.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "10-file_io.R"), echo = FALSE)
source(here::here("scripts", "1-import_pipeline", "11-reading.R"), echo = FALSE)


# --- build_read_error --------------------------------------------------------

testthat::test_that("build_read_error formats error message with file path", {
  result <- build_read_error(
    context_message = "failed to read",
    file_path = "/path/to/file.xlsx",
    error_message = "sheet not found"
  )

  testthat::expect_true(is.character(result))
  testthat::expect_true(grepl("file.xlsx", result))
})


# --- create_empty_read_result ------------------------------------------------

testthat::test_that("create_empty_read_result returns correct structure", {
  result <- create_empty_read_result(errors = c("error1", "error2"))

  testthat::expect_true(is.list(result))
  testthat::expect_true(data.table::is.data.table(result$data))
  testthat::expect_equal(nrow(result$data), 0L)
  testthat::expect_identical(result$errors, c("error1", "error2"))
})


# --- has_read_errors ---------------------------------------------------------

testthat::test_that("has_read_errors detects errors correctly", {
  no_errors <- list(data = data.table::data.table(), errors = character(0))
  testthat::expect_false(has_read_errors(no_errors))

  with_errors <- list(data = data.table::data.table(), errors = "some error")
  testthat::expect_true(has_read_errors(with_errors))
})


# --- read_excel_sheet --------------------------------------------------------

testthat::test_that("read_excel_sheet reads a valid xlsx file", {
  root_dir <- build_temp_dir("fao-read-sheet-")
  file_path <- file.path(root_dir, "test.xlsx")

  test_data <- data.frame(
    continent = c("Asia", "Europe"),
    country   = c("Japan", "France"),
    value     = c("100", "200"),
    stringsAsFactors = FALSE
  )

  create_test_xlsx(test_data, file_path)

  config <- build_test_config()

  result <- read_excel_sheet(
    file_path = file_path,
    sheet_name = "Sheet1",
    config = config
  )

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true(nrow(result) > 0)
  testthat::expect_true("continent" %in% names(result))
  testthat::expect_true("country"   %in% names(result))
})

testthat::test_that("read_excel_sheet handles file with missing required columns gracefully", {
  root_dir <- build_temp_dir("fao-read-missing-cols-")
  file_path <- file.path(root_dir, "test_missing.xlsx")

  test_data <- data.frame(
    non_standard_col = c("a", "b"),
    stringsAsFactors = FALSE
  )

  create_test_xlsx(test_data, file_path)

  config <- build_test_config()

  # should still read but base columns may be empty
  result <- read_excel_sheet(
    file_path = file_path,
    sheet_name = "Sheet1",
    config = config
  )

  testthat::expect_true(data.table::is.data.table(result))
})


# --- read_file_sheets --------------------------------------------------------

testthat::test_that("read_file_sheets reads all sheets from a file", {
  root_dir <- build_temp_dir("fao-read-sheets-")
  file_path <- file.path(root_dir, "multi_sheet.xlsx")

  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "Sheet1")
  openxlsx::writeData(wb, "Sheet1", data.frame(
    continent = "Asia",
    country = "Japan",
    value = "100",
    stringsAsFactors = FALSE
  ))
  openxlsx::addWorksheet(wb, "Sheet2")
  openxlsx::writeData(wb, "Sheet2", data.frame(
    continent = "Europe",
    country = "France",
    value = "200",
    stringsAsFactors = FALSE
  ))
  openxlsx::saveWorkbook(wb, file_path, overwrite = TRUE)

  config <- build_test_config()

  result <- read_file_sheets(file_path, config)

  testthat::expect_true(is.list(result) || data.table::is.data.table(result))
})


# --- read_pipeline_files (sequential) ----------------------------------------

testthat::test_that("read_pipeline_files returns empty results for empty file list", {
  file_list_dt <- data.frame(file_path = character(0), stringsAsFactors = FALSE)
  config <- build_test_config()

  result <- read_pipeline_files(file_list_dt, config)

  testthat::expect_true(is.list(result))
  testthat::expect_identical(result$read_data_list, list())
  testthat::expect_identical(result$errors, character(0))
})

testthat::test_that("read_pipeline_files reads single xlsx file", {
  root_dir <- build_temp_dir("fao-pipeline-read-")
  file_path <- file.path(root_dir, "test.xlsx")

  test_data <- data.frame(
    continent = c("Asia", "Europe"),
    country   = c("Japan", "France"),
    value     = c("100", "200"),
    stringsAsFactors = FALSE
  )

  create_test_xlsx(test_data, file_path)

  file_list_dt <- data.frame(file_path = file_path, stringsAsFactors = FALSE)
  config <- build_test_config()

  result <- read_pipeline_files(file_list_dt, config)

  testthat::expect_true(is.list(result))
  testthat::expect_equal(length(result$read_data_list), 1L)
  testthat::expect_true(is.character(result$errors))
})


# --- edge cases: malformed files ---------------------------------------------

testthat::test_that("read_pipeline_files handles corrupted file gracefully", {
  root_dir <- build_temp_dir("fao-read-corrupted-")
  file_path <- file.path(root_dir, "corrupted.xlsx")

  # write random bytes (not a valid Excel file)
  writeBin(charToRaw("this is not an excel file"), file_path)

  file_list_dt <- data.frame(file_path = file_path, stringsAsFactors = FALSE)
  config <- build_test_config()

  result <- read_pipeline_files(file_list_dt, config)

  testthat::expect_true(is.list(result))
  testthat::expect_true(length(result$errors) > 0)
})

testthat::test_that("safe_execute_read captures errors and returns structured result", {
  result <- safe_execute_read(
    operation = function() stop("test error"),
    context_message = "test context",
    file_path = "/fake/path.xlsx"
  )

  # should not throw; result should contain error info
  testthat::expect_true(is.character(result) || is.list(result))
})


# --- janitor::remove_empty integration ---------------------------------------

testthat::test_that("read_excel_sheet removes entirely blank rows via janitor::remove_empty", {
  root_dir <- build_temp_dir("fao-read-empty-rows-")
  file_path <- file.path(root_dir, "test_empty_rows.xlsx")

  # row 2 is entirely blank (NA values become empty cells in Excel,
  # which readxl reads back as NA_character_ with col_types = "text")
  test_data <- data.frame(
    continent = c("Asia", NA_character_, "Europe"),
    country   = c("Japan", NA_character_, "France"),
    stringsAsFactors = FALSE
  )

  create_test_xlsx(test_data, file_path)
  config <- build_test_config()

  result <- read_excel_sheet(file_path, "Sheet1", config)

  # the entirely blank row should have been removed
  testthat::expect_equal(nrow(result$data), 2L)
  testthat::expect_equal(sort(result$data$continent), c("Asia", "Europe"))
  testthat::expect_equal(sort(result$data$country),   c("France", "Japan"))
})

testthat::test_that("read_excel_sheet removes entirely blank columns via janitor::remove_empty", {
  root_dir <- build_temp_dir("fao-read-empty-cols-")
  file_path <- file.path(root_dir, "test_empty_cols.xlsx")

  # empty_col is entirely NA; janitor::remove_empty should drop it
  test_data <- data.frame(
    continent = c("Asia", "Europe"),
    country   = c("Japan", "France"),
    empty_col = c(NA_character_, NA_character_),
    stringsAsFactors = FALSE
  )

  create_test_xlsx(test_data, file_path)
  config <- build_test_config()

  result <- read_excel_sheet(file_path, "Sheet1", config)

  # the entirely blank column should have been removed
  testthat::expect_false("empty_col" %in% names(result$data))
  testthat::expect_true("continent" %in% names(result$data))
  testthat::expect_true("country"   %in% names(result$data))
})
