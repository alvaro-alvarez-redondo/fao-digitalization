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

testthat::test_that("read_file_sheets returns structured errors for missing file", {
  missing_path <- fs::path(withr::local_tempdir(), "missing.xlsx")

  result <- read_file_sheets(missing_path, test_config)

  testthat::expect_true(data.table::is.data.table(result$data))
  testthat::expect_equal(nrow(result$data), 0)
  testthat::expect_type(result$errors, "character")
  testthat::expect_length(result$errors, 1)
  testthat::expect_match(result$errors[[1]], "failed to list sheets", ignore.case = TRUE)
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


testthat::test_that("transform_files_list returns empty tables when all transformed files are null", {
  file_list_dt <- data.table::data.table(
    file_name = c("a.xlsx", "b.xlsx"),
    yearbook = c("yb_2020", "yb_2021"),
    product = c("rice", "wheat")
  )

  read_data_list <- list(data.table::data.table(), data.table::data.table())

  transformed <- transform_files_list(file_list_dt, read_data_list, test_config)

  testthat::expect_true(data.table::is.data.table(transformed$wide_raw))
  testthat::expect_true(data.table::is.data.table(transformed$long_raw))
  testthat::expect_equal(nrow(transformed$wide_raw), 0)
  testthat::expect_equal(nrow(transformed$long_raw), 0)
})

testthat::test_that("process_files fails fast when read_data_list contains non-tabular elements", {
  file_list_dt <- data.table::data.table(
    file_name = "a.xlsx",
    yearbook = "yb_2020",
    product = "rice"
  )

  invalid_read_data_list <- list(list(invalid = TRUE))

  testthat::expect_error(
    process_files(file_list_dt, invalid_read_data_list, test_config),
    regexp = "read_data_list"
  )
})

testthat::test_that("process_files fails fast when read_data_list length differs from file metadata rows", {
  file_list_dt <- data.table::data.table(
    file_name = c("a.xlsx", "b.xlsx"),
    yearbook = c("yb_2020", "yb_2021"),
    product = c("rice", "wheat")
  )

  read_data_list <- list(
    data.table::data.table(
      variable = "production",
      continent = "asia",
      country = "nepal",
      unit = "t",
      footnotes = "none",
      `2020` = "1"
    )
  )

  testthat::expect_error(
    process_files(file_list_dt, read_data_list, test_config),
    regexp = "length must match"
  )
})

testthat::test_that("read_pipeline_files returns stable empty output for empty file metadata", {
  empty_file_list <- data.table::data.table(file_path = character(0))

  result <- read_pipeline_files(empty_file_list, test_config)

  testthat::expect_type(result, "list")
  testthat::expect_identical(result$read_data_list, list())
  testthat::expect_identical(result$errors, character(0))
})

testthat::test_that("read_excel_sheet validates config base columns", {
  testthat::expect_error(
    read_excel_sheet(
      file_path = "dummy.xlsx",
      sheet_name = "sheet1",
      config = list(column_required = character(0))
    ),
    regexp = "column_required|min.len"
  )
})

testthat::test_that("read_excel_sheet returns standardized error for unreadable sheet", {
  missing_path <- fs::path(withr::local_tempdir(), "missing.xlsx")

  result <- read_excel_sheet(
    file_path = missing_path,
    sheet_name = "sheet1",
    config = test_config
  )

  testthat::expect_true(data.table::is.data.table(result$data))
  testthat::expect_equal(nrow(result$data), 0)
  testthat::expect_type(result$errors, "character")
  testthat::expect_true(any(grepl("failed to read sheet", result$errors, ignore.case = TRUE)))
})

testthat::test_that("read_pipeline_files keeps output length stable on internal read errors", {
  file_list_dt <- data.table::data.table(file_path = c("file_a.xlsx", "file_b.xlsx"))

  testthat::local_mocked_bindings(
    read_file_sheets = function(file_path, config) {
      if (identical(file_path, "file_a.xlsx")) {
        stop("unexpected read failure")
      }

      list(data = data.table::data.table(id = 1L), errors = character(0))
    }
  )

  result <- read_pipeline_files(file_list_dt, test_config)

  testthat::expect_length(result$read_data_list, 2)
  testthat::expect_true(data.table::is.data.table(result$read_data_list[[1]]))
  testthat::expect_equal(nrow(result$read_data_list[[1]]), 0)
  testthat::expect_true(any(grepl("failed to read pipeline file", result$errors, ignore.case = TRUE)))
})

testthat::test_that("safe_execute_read returns result and no errors on successful operation", {
  result <- safe_execute_read(
    operation = function() 42L,
    context_message = "failed operation",
    file_path = "file.xlsx"
  )

  testthat::expect_identical(result$result, 42L)
  testthat::expect_identical(result$errors, character(0))
})

testthat::test_that("safe_execute_read captures operation failures with standardized context", {
  result <- safe_execute_read(
    operation = function() stop("internal failure"),
    context_message = "failed operation",
    file_path = "file.xlsx"
  )

  testthat::expect_null(result$result)
  testthat::expect_type(result$errors, "character")
  testthat::expect_length(result$errors, 1)
  testthat::expect_true(any(grepl("failed operation", result$errors, ignore.case = TRUE)))
})

testthat::test_that("safe_execute_read validates operation input", {
  testthat::expect_error(
    safe_execute_read(
      operation = 1L,
      context_message = "failed operation",
      file_path = "file.xlsx"
    ),
    class = "rlang_error"
  )
})

testthat::test_that("build_read_error keeps stable output structure", {
  formatted_error <- build_read_error(
    context_message = "failed to read",
    file_path = "folder/file.xlsx",
    details = "sheet not found"
  )

  testthat::expect_type(formatted_error, "character")
  testthat::expect_length(formatted_error, 1)
  testthat::expect_true(any(grepl("file\\.xlsx", formatted_error, ignore.case = TRUE)))
  testthat::expect_true(any(grepl("sheet not found", formatted_error, ignore.case = TRUE)))
})

testthat::test_that("read_pipeline_files uses map_with_progress wrapper", {
  file_list_dt <- data.table::data.table(file_path = "file_a.xlsx")

  testthat::local_mocked_bindings(
    map_with_progress = function(x, .f, ..., message_template = NULL, message_fn = NULL, enable_progress = getOption("fao.progress.enabled", TRUE)) {
      testthat::expect_match(message_template, "reading file")
      purrr::map(x, .f, ...)
    },
    read_file_sheets = function(file_path, config) {
      list(data = data.table::data.table(id = 1L), errors = character(0))
    }
  )

  result <- read_pipeline_files(file_list_dt, test_config)

  testthat::expect_length(result$read_data_list, 1)
  testthat::expect_equal(nrow(result$read_data_list[[1]]), 1)
})

testthat::test_that("read helpers return standardized empty structures", {
  output <- create_empty_read_result(c("error a"))

  testthat::expect_true(data.table::is.data.table(output$data))
  testthat::expect_identical(output$errors, c("error a"))
  testthat::expect_true(has_read_errors(list(result = NULL, errors = "x")))
  testthat::expect_false(has_read_errors(list(result = 1L, errors = character(0))))
})

testthat::test_that("run_export_pipeline deletes stale audit root before exporting", {
  withr::local_options(list(
    fao.run_export_pipeline.auto = FALSE,
    fao.run_import_pipeline.auto = FALSE
  ))
  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

  temp_root <- withr::local_tempdir()
  raw_imports_dir <- fs::path(temp_root, "raw imports")
  audit_root_dir <- fs::path(temp_root, "audit")

  fs::dir_create(raw_imports_dir, recurse = TRUE)
  fs::dir_create(fs::path(audit_root_dir, "old_dataset"), recurse = TRUE)
  fs::file_create(fs::path(audit_root_dir, "old_dataset", "stale.xlsx"))

  input_dt <- data.table::data.table(
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
    document = "sample.xlsx"
  )

  config <- test_config
  config$paths$data$imports$raw <- raw_imports_dir
  config$paths$data$audit$audit_root_dir <- audit_root_dir
  config$paths$data$audit$audit_file_path <- fs::path(
    audit_root_dir,
    "new_dataset",
    "new_dataset_audit.xlsx"
  )
  config$paths$data$audit$raw_imports_mirror_dir <- fs::path(
    audit_root_dir,
    "new_dataset",
    "raw_imports_mirror"
  )

  fs::file_create(fs::path(raw_imports_dir, "sample.xlsx"))

  output <- run_export_pipeline(input_dt, config, overwrite = TRUE)

  testthat::expect_true(fs::file_exists(output$processed_path))
  testthat::expect_false(fs::dir_exists(fs::path(audit_root_dir, "old_dataset")))
})
