source(here::here("R/3-export_pipeline/30-data_audit.R"), echo = FALSE)

testthat::test_that("identify_audit_errors returns empty table when audit columns have no missing values", {
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

  audit_dt <- identify_audit_errors(input_dt, test_config)

  testthat::expect_true(data.table::is.data.table(audit_dt))
  testthat::expect_equal(nrow(audit_dt), 0)
})

testthat::test_that("identify_audit_errors includes rows with missing continent country or product", {
  input_dt <- data.table::data.table(
    continent = c("asia", NA_character_, "africa"),
    country = c("nepal", "kenya", NA_character_),
    product = c("rice", "maize", "wheat"),
    variable = "production",
    unit = "t",
    year = "2020",
    value = "1",
    notes = NA_character_,
    footnotes = "none",
    yearbook = "yb_2020",
    document = c("clean.xlsx", "missing_continent.xlsx", "missing_country.xlsx")
  )

  audit_result <- identify_audit_errors(
    input_dt,
    test_config,
    include_findings = TRUE
  )

  testthat::expect_equal(nrow(audit_result$audit_dt), 2)
  testthat::expect_identical(
    audit_result$audit_dt$document,
    c("missing_continent.xlsx", "missing_country.xlsx")
  )
  testthat::expect_identical(
    audit_result$findings_dt$audit_column,
    c("continent", "country")
  )
  testthat::expect_identical(audit_result$findings_dt$row_index, c(2L, 3L))
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
  export_validation_audit_report(
    audit_dt = audit_dt,
    config = test_config,
    output_path = output_path
  )

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
  export_validation_audit_report(
    audit_dt = audit_dt,
    config = test_config,
    output_path = output_path
  )

  exported_dt <- openxlsx::read.xlsx(output_path) |>
    data.table::as.data.table()

  testthat::expect_identical(exported_dt$document, c("alpha.xlsx", "zeta.xlsx"))
})

testthat::test_that("audit_data_output warns and creates report for rows with missing audit columns", {
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

testthat::test_that("clear_audit_output_directory deletes existing audit tree", {
  temp_root <- withr::local_tempdir()
  audit_dir <- fs::path(temp_root, "audit", "dataset_a")
  stale_file <- fs::path(audit_dir, "stale.xlsx")

  fs::dir_create(audit_dir, recurse = TRUE)
  fs::file_create(stale_file)

  deleted <- clear_audit_output_directory(audit_dir)

  testthat::expect_true(deleted)
  testthat::expect_false(fs::dir_exists(audit_dir))
})

testthat::test_that("audit_data_output clears stale audit outputs and does not recreate folder when clean", {
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

  config <- test_config
  temp_root <- withr::local_tempdir()
  audit_dir <- fs::path(temp_root, "audit", "clean_dataset")
  stale_file <- fs::path(audit_dir, "stale.xlsx")

  fs::dir_create(audit_dir, recurse = TRUE)
  fs::file_create(stale_file)

  config$paths$data$audit$audit_root_dir <- fs::path(temp_root, "audit")
  config$paths$data$audit$audit_file_path <- fs::path(
    config$paths$data$audit$audit_root_dir,
    "clean_dataset",
    "clean_dataset_audit.xlsx"
  )
  config$paths$data$audit$raw_imports_mirror_dir <- fs::path(
    config$paths$data$audit$audit_root_dir,
    "clean_dataset",
    "raw_imports_mirror"
  )

  audited_dt <- audit_data_output(input_dt, config)

  testthat::expect_true(data.table::is.data.table(audited_dt))
  testthat::expect_false(fs::dir_exists(audit_dir))
})

testthat::test_that("load_audit_config rejects missing required fields", {
  invalid_config <- list(column_order = "document")

  testthat::expect_error(
    load_audit_config(invalid_config),
    class = "rlang_error"
  )
})

testthat::test_that("export_validation_audit_report returns output path", {
  audit_dt <- data.table::data.table(document = "sample.xlsx")
  output_path <- fs::path(withr::local_tempdir(), "audit_path.xlsx")

  saved_path <- export_validation_audit_report(
    audit_dt = audit_dt,
    config = test_config,
    output_path = output_path
  )

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


testthat::test_that("load_pipeline_config includes centralized error highlight style", {
  config <- load_pipeline_config("fao_data_raw")

  testthat::expect_identical(
    config$export_config$styles$error_highlight$fgFill,
    "#ffff00"
  )
  testthat::expect_identical(
    config$export_config$styles$error_highlight$fontColour,
    "#000000"
  )
  testthat::expect_identical(
    config$export_config$styles$error_highlight$textDecoration,
    "bold"
  )
})

testthat::test_that("identify_audit_errors can return detailed findings object", {
  input_dt <- data.table::data.table(
    continent = c(NA_character_, "asia"),
    country = c("nepal", "india"),
    product = c("rice", "wheat"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020", "2021"),
    value = c("1", "2"),
    notes = c(NA_character_, NA_character_),
    footnotes = c("none", "none"),
    yearbook = c("yb_2020", "yb_2021"),
    document = c("a.xlsx", "b.xlsx")
  )

  audit_result <- identify_audit_errors(
    input_dt,
    test_config,
    include_findings = TRUE
  )

  testthat::expect_named(audit_result, c("audit_dt", "findings_dt"))
  testthat::expect_true(data.table::is.data.table(audit_result$audit_dt))
  testthat::expect_true(data.table::is.data.table(audit_result$findings_dt))
  testthat::expect_true(all(
    c("row_index", "audit_column") %in% names(audit_result$findings_dt)
  ))
})
