testthat::test_that("identify_validation_errors returns empty table for clean data", {
  input_dt <- data.table::data.table(
    continent = "asia",
    country = "nepal",
    product = "rice",
    variable = "production",
    unit = "t",
    year = "2020",
    value = "1.25",
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = "sample.xlsx"
  )

  audit_dt <- identify_validation_errors(input_dt)

  testthat::expect_true(data.table::is.data.table(audit_dt))
  testthat::expect_equal(nrow(audit_dt), 0)
})

testthat::test_that("identify_validation_errors captures multiple invalid columns", {
  input_dt <- data.table::data.table(
    continent = c("asia", "asia "),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020/2021", "2020"),
    value = c("not_numeric", "-1"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("sample.xlsx", "sample.xlsx")
  )

  audit_dt <- identify_validation_errors(input_dt)

  testthat::expect_equal(nrow(audit_dt), 2)
  testthat::expect_identical(colnames(audit_dt)[1], "error_columns")
  testthat::expect_true(all(nzchar(audit_dt$error_columns)))
  testthat::expect_true(any(grepl("value", audit_dt$error_columns)))
  testthat::expect_true(any(grepl("year", audit_dt$error_columns)))
})

testthat::test_that("identify_validation_errors flags duplicated keys", {
  input_dt <- data.table::data.table(
    continent = c("asia", "asia"),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020", "2020"),
    value = c("1", "2"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("sample.xlsx", "sample.xlsx")
  )

  audit_dt <- identify_validation_errors(input_dt)

  testthat::expect_equal(nrow(audit_dt), 2)
  testthat::expect_true(all(grepl("document", audit_dt$error_columns)))
})

testthat::test_that("export_validation_audit_report writes excel report", {
  audit_dt <- data.table::data.table(
    error_columns = "year,value",
    continent = "asia",
    country = "nepal",
    product = "rice",
    variable = "production",
    unit = "t",
    year = "2020/2021",
    value = "not_numeric",
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = "sample.xlsx"
  )

  output_path <- fs::path(withr::local_tempdir(), "audit.xlsx")
  saved_path <- export_validation_audit_report(audit_dt, output_path)

  testthat::expect_true(fs::file_exists(saved_path))
})

testthat::test_that("validate_data aborts and creates report for dirty rows", {
  input_dt <- data.table::data.table(
    continent = "asia",
    country = "nepal",
    product = "rice",
    variable = "production",
    unit = "t",
    year = "2020/2021",
    value = "not_numeric",
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = "sample.xlsx"
  )

  output_path <- fs::path(withr::local_tempdir(), "validation_audit.xlsx")

  testthat::expect_error(
    validate_data(input_dt, output_path = output_path),
    regexp = "data validation failed"
  )

  testthat::expect_true(fs::file_exists(output_path))
})

testthat::test_that("validate_data returns numeric value for clean rows", {
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

  validated_dt <- validate_data(input_dt)

  testthat::expect_true(data.table::is.data.table(validated_dt))
  testthat::expect_type(validated_dt$value, "double")
  testthat::expect_identical(validated_dt$value[[1]], 1.25)
})
