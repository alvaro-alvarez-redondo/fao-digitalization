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
  testthat::expect_identical(audit_dt$document, c("sample.xlsx", "sample.xlsx"))
})


testthat::test_that("identify_validation_errors sorts output by document", {
  input_dt <- data.table::data.table(
    continent = c("asia", "asia"),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020/2021", "2020/2021"),
    value = c("2", "not_numeric"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("zeta.xlsx", "alpha.xlsx")
  )

  audit_dt <- identify_validation_errors(input_dt)

  testthat::expect_equal(nrow(audit_dt), 2)
  testthat::expect_identical(audit_dt$document, c("alpha.xlsx", "zeta.xlsx"))
})

testthat::test_that("identify_validation_errors does not add an error_columns field", {
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

  audit_dt <- identify_validation_errors(input_dt)

  testthat::expect_false("error_columns" %in% names(audit_dt))
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
  testthat::expect_true(all(audit_dt$document == "sample.xlsx"))
})

testthat::test_that("export_validation_audit_report writes excel report", {
  audit_dt <- data.table::data.table(
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

testthat::test_that("export_validation_audit_report sorts audit rows by document", {
  audit_dt <- data.table::data.table(
    continent = c("asia", "asia"),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020/2021", "2020"),
    value = c("1", "not_numeric"),
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


testthat::test_that("validate_data warns and creates report for dirty rows", {
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

  validated_dt <- testthat::expect_warning(
    validate_data(input_dt, config = config),
    regexp = "data validation failed"
  )

  testthat::expect_true(data.table::is.data.table(validated_dt))
  testthat::expect_type(validated_dt$value, "double")
  testthat::expect_true(is.na(validated_dt$value[[1]]))

  testthat::expect_true(fs::file_exists(
    config$paths$data$audit$audit_file_path
  ))
  testthat::expect_true(
    fs::file_exists(fs::path(
      config$paths$data$audit$raw_imports_mirror_dir,
      "sample.xlsx"
    ))
  )
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

  validated_dt <- validate_data(input_dt, test_config)

  testthat::expect_true(data.table::is.data.table(validated_dt))
  testthat::expect_type(validated_dt$value, "double")
  testthat::expect_identical(validated_dt$value[[1]], 1.25)
})
