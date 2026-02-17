source(here::here("R/1-import_pipeline/14-data_audit.R"), echo = FALSE)

testthat::test_that("audit_character_non_empty_column flags missing empty and whitespace strings", {
  input_dt <- data.table::data.table(continent = c("asia", "", "  ", NA_character_))

  invalid_rows <- audit_character_non_empty_column(input_dt, "continent")

  testthat::expect_identical(invalid_rows, c(FALSE, TRUE, TRUE, TRUE))
})

testthat::test_that("audit_character_non_empty_column validates character input", {
  input_dt <- data.table::data.table(continent = c(1, 2))

  testthat::expect_error(
    audit_character_non_empty_column(input_dt, "continent"),
    class = "rlang_error"
  )
})

testthat::test_that("audit_numeric_string_column validates strict numeric strings", {
  input_dt <- data.table::data.table(value = c("12", "12.4", "12a", " 2", "1.2.3", NA_character_))

  invalid_rows <- audit_numeric_string_column(input_dt, "value")

  testthat::expect_identical(invalid_rows, c(FALSE, FALSE, TRUE, TRUE, TRUE, TRUE))
})

testthat::test_that("run_column_audits returns structured audit result", {
  input_dt <- data.table::data.table(
    continent = c("asia", NA_character_),
    country = c("nepal", ""),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020", "2020"),
    value = c("1", "bad"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("clean.xlsx", "dirty.xlsx")
  )

  audit_result <- run_column_audits(input_dt, test_config)

  testthat::expect_true(is.list(audit_result))
  testthat::expect_true(is.logical(audit_result$invalid_rows))
  testthat::expect_true(data.table::is.data.table(audit_result$summary))
  testthat::expect_true(any(audit_result$summary$audit_type == "numeric_string"))
})

testthat::test_that("identify_audit_errors returns empty table when all audited columns are valid", {
  input_dt <- data.table::data.table(
    continent = "asia",
    country = "nepal",
    product = "rice",
    variable = "production",
    unit = "t",
    year = "2020/2021",
    value = "12.25",
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = "sample.xlsx"
  )

  audit_dt <- identify_audit_errors(input_dt, test_config)

  testthat::expect_true(data.table::is.data.table(audit_dt))
  testthat::expect_equal(nrow(audit_dt), 0)
})

testthat::test_that("identify_audit_errors includes rows with invalid character or value columns", {
  input_dt <- data.table::data.table(
    continent = c("asia", NA_character_, "africa"),
    country = c("nepal", "kenya", NA_character_),
    product = c("rice", "maize", "wheat"),
    variable = "production",
    unit = "t",
    year = "2020",
    value = c("1", "2", "bad"),
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = c("clean.xlsx", "missing_continent.xlsx", "invalid_value.xlsx")
  )

  audit_dt <- identify_audit_errors(input_dt, test_config)

  testthat::expect_equal(nrow(audit_dt), 2)
  testthat::expect_identical(
    audit_dt$document,
    c("invalid_value.xlsx", "missing_continent.xlsx")
  )
})

testthat::test_that("identify_audit_errors sorts output by document", {
  input_dt <- data.table::data.table(
    continent = c(NA_character_, NA_character_),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020", "2020"),
    value = c("1", "2"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("zeta.xlsx", "alpha.xlsx")
  )

  audit_dt <- identify_audit_errors(input_dt, test_config)

  testthat::expect_equal(nrow(audit_dt), 2)
  testthat::expect_identical(audit_dt$document, c("alpha.xlsx", "zeta.xlsx"))
})

testthat::test_that("identify_audit_errors validates audit column types", {
  input_dt <- data.table::data.table(
    continent = 1,
    country = "nepal",
    product = "rice",
    value = "1",
    document = "sample.xlsx"
  )

  testthat::expect_error(
    identify_audit_errors(input_dt, test_config),
    class = "rlang_error"
  )
})

testthat::test_that("export_validation_audit_report writes excel report", {
  audit_dt <- data.table::data.table(
    continent = NA_character_,
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

  output_path <- fs::path(withr::local_tempdir(), "audit.xlsx")
  export_validation_audit_report(audit_dt, output_path)

  testthat::expect_true(fs::file_exists(output_path))
})

testthat::test_that("export_validation_audit_report sorts audit rows by document", {
  audit_dt <- data.table::data.table(
    continent = c(NA_character_, NA_character_),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020/2021", "2020"),
    value = c("1", "2"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("zeta.xlsx", "alpha.xlsx")
  )

  output_path <- fs::path(withr::local_tempdir(), "audit_sorted.xlsx")
  export_validation_audit_report(audit_dt, output_path)

  exported_dt <- openxlsx::read.xlsx(output_path) |>
    data.table::as.data.table()

  testthat::expect_identical(exported_dt$document, c("alpha.xlsx", "zeta.xlsx"))
})

testthat::test_that("audit_data_output creates report for rows with audit failures", {
  input_dt <- data.table::data.table(
    continent = NA_character_,
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

  temp_raw_imports <- fs::path(withr::local_tempdir(), "raw imports")
  fs::dir_create(temp_raw_imports)
  fs::file_create(fs::path(temp_raw_imports, "sample.xlsx"))

  config <- test_config
  config$paths$data$imports$raw <- temp_raw_imports
  config$paths$data$audit$audit_dir <- fs::path(
    withr::local_tempdir(),
    "audit",
    config$dataset_name
  )
  config$paths$data$audit$dataset_dir <- config$paths$data$audit$audit_dir
  config$paths$data$audit$audit_file_path <- fs::path(
    config$paths$data$audit$audit_dir,
    paste0(config$dataset_name, "_audit.xlsx")
  )
  config$paths$data$audit$raw_imports_mirror_dir <- fs::path(
    config$paths$data$audit$audit_dir,
    "raw_imports_mirror"
  )

  audited_dt <- audit_data_output(input_dt, config = config)

  testthat::expect_true(data.table::is.data.table(audited_dt))
  testthat::expect_type(audited_dt$value, "double")
  testthat::expect_identical(audited_dt$value[[1]], 1)

  testthat::expect_true(fs::file_exists(config$paths$data$audit$audit_file_path))
  testthat::expect_true(
    fs::file_exists(fs::path(config$paths$data$audit$raw_imports_mirror_dir, "sample.xlsx"))
  )
})

testthat::test_that("audit_data_output returns numeric value for clean rows", {
  input_dt <- data.table::data.table(
    continent = "asia",
    country = "nepal",
    product = "rice",
    variable = "production",
    unit = "t",
    year = "2020-2021",
    value = "1.25",
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = "sample.xlsx"
  )

  audited_dt <- audit_data_output(input_dt, test_config)

  testthat::expect_true(data.table::is.data.table(audited_dt))
  testthat::expect_type(audited_dt$value, "double")
  testthat::expect_identical(audited_dt$value[[1]], 1.25)
})

testthat::test_that("validate_audit_config rejects missing required fields", {
  invalid_config <- list(column_order = "document")

  testthat::expect_error(
    validate_audit_config(invalid_config),
    class = "rlang_error"
  )
})

testthat::test_that("export_validation_audit_report returns output path", {
  audit_dt <- data.table::data.table(document = "sample.xlsx")
  output_path <- fs::path(withr::local_tempdir(), "audit_path.xlsx")

  saved_path <- export_validation_audit_report(audit_dt, output_path)

  testthat::expect_identical(saved_path, output_path)
})

testthat::test_that("mirror_raw_import_errors returns empty when no document matches", {
  audit_dt <- data.table::data.table(document = "unknown.xlsx")
  raw_imports_dir <- fs::path(withr::local_tempdir(), "raw_imports")
  raw_imports_mirror_dir <- fs::path(withr::local_tempdir(), "mirror")

  fs::dir_create(raw_imports_dir)
  fs::file_create(fs::path(raw_imports_dir, "known.xlsx"))

  mirrored_paths <- mirror_raw_import_errors(
    audit_dt = audit_dt,
    raw_imports_dir = raw_imports_dir,
    raw_imports_mirror_dir = raw_imports_mirror_dir
  )

  testthat::expect_identical(mirrored_paths, character(0))
})
