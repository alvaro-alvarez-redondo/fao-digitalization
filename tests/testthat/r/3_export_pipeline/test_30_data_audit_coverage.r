source(here::here("R/2-post_processing_pipeline/20-data_audit.R"), echo = FALSE)

testthat::test_that("load_audit_config accepts valid config and remains backward compatible", {
  config <- load_pipeline_config("fao_data_raw")

  output_flag <- load_audit_config(config)

  testthat::expect_identical(output_flag, TRUE)
})

testthat::test_that("load_audit_config rejects invalid config", {
  invalid_config <- list(
    column_order = "document",
    audit_columns = "document",
    paths = list(data = list(imports = list(raw = "tmp"), audit = list()))
  )

  testthat::expect_error(
    load_audit_config(invalid_config),
    class = "rlang_error"
  )
})

testthat::test_that("run_master_validation handles happy path, edge case, and error case", {
  dataset_dt <- data.frame(
    document = c("ok.xlsx", "bad.xlsx"),
    value = c("10", "10a"),
    stringsAsFactors = FALSE
  )

  audit_columns_by_type <- list(
    character_non_empty = "document",
    numeric_string = "value"
  )

  output_list <- run_master_validation(dataset_dt, audit_columns_by_type)
  empty_output <- run_master_validation(data.frame(document = character(), value = character()), audit_columns_by_type)

  testthat::expect_true(is.list(output_list))
  testthat::expect_true("numeric_string" %in% output_list$findings$audit_type)
  testthat::expect_identical(empty_output$invalid_row_index, integer(0))

  testthat::expect_error(
    run_master_validation(dataset_dt, audit_columns_by_type, selected_validations = 1),
    class = "rlang_error"
  )
})

testthat::test_that("export_validation_audit_report supports happy path, edge path, and backward compatibility", {
  config <- load_pipeline_config("fao_data_raw")
  output_path <- fs::path(withr::local_tempdir(), "audit_report.xlsx")

  audit_dt <- data.table::data.table(
    document = c("b.xlsx", "a.xlsx"),
    row_index = c(2L, 1L),
    audit_column = c("document", "document"),
    audit_type = c("character_non_empty", "character_non_empty"),
    audit_message = c("bad", "bad")
  )

  findings_dt <- data.table::data.table(
    row_index = c(1L, 2L),
    audit_column = c("document", "document")
  )

  saved_path <- export_validation_audit_report(
    audit_dt = audit_dt,
    config = config,
    findings_dt = findings_dt,
    output_path = output_path
  )

  saved_path_fallback <- export_validation_audit_report(
    audit_dt = audit_dt,
    config = config,
    findings_dt = NULL,
    output_path = output_path
  )

  empty_saved <- export_validation_audit_report(
    audit_dt = data.table::data.table(document = character()),
    config = config,
    output_path = output_path
  )

  testthat::expect_identical(saved_path, output_path)
  testthat::expect_identical(saved_path_fallback, output_path)
  testthat::expect_true(fs::file_exists(output_path))
  testthat::expect_null(empty_saved)

  testthat::expect_error(
    export_validation_audit_report(
      audit_dt = audit_dt,
      config = config,
      output_path = ""
    ),
    class = "rlang_error"
  )
})

testthat::test_that("mirror_raw_import_errors copies matched files and handles edge/error cases", {
  raw_imports_dir <- fs::path(withr::local_tempdir(), "raw")
  mirror_dir <- fs::path(withr::local_tempdir(), "mirror")
  nested_dir <- fs::path(raw_imports_dir, "nested")

  fs::dir_create(nested_dir, recurse = TRUE)
  matched_file <- fs::path(nested_dir, "matched.xlsx")
  fs::file_create(matched_file)

  audit_dt <- data.table::data.table(document = "matched.xlsx")

  mirrored_paths <- mirror_raw_import_errors(
    audit_dt = audit_dt,
    raw_imports_dir = raw_imports_dir,
    raw_imports_mirror_dir = mirror_dir
  )

  edge_paths <- mirror_raw_import_errors(
    audit_dt = data.table::data.table(document = character()),
    raw_imports_dir = raw_imports_dir,
    raw_imports_mirror_dir = mirror_dir
  )

  testthat::expect_length(mirrored_paths, 1)
  testthat::expect_true(fs::file_exists(mirrored_paths[[1]]))
  testthat::expect_match(fs::path_rel(mirrored_paths[[1]], start = mirror_dir), "nested/matched\\.xlsx$")
  testthat::expect_identical(edge_paths, character(0))

  testthat::expect_error(
    mirror_raw_import_errors(
      audit_dt = audit_dt,
      raw_imports_dir = fs::path(withr::local_tempdir(), "missing"),
      raw_imports_mirror_dir = mirror_dir
    ),
    class = "rlang_error"
  )
})

testthat::test_that("audit_data_output preserves return structure and compatibility", {
  config <- load_pipeline_config("fao_data_raw")
  temp_root <- withr::local_tempdir()

  config$paths$data$imports$raw <- fs::path(temp_root, "raw-imports")
  config$paths$data$audit$audit_root_dir <- fs::path(temp_root, "audit")
  config$paths$data$audit$audit_file_path <- fs::path(temp_root, "audit", "report.xlsx")
  config$paths$data$audit$raw_imports_mirror_dir <- fs::path(temp_root, "audit", "mirror")

  fs::dir_create(config$paths$data$imports$raw, recurse = TRUE)

  input_dt <- data.table::data.table(
    continent = c("asia", "asia"),
    country = c("nepal", "india"),
    product = c("rice", "wheat"),
    variable = c("production", "production"),
    unit = c("t", "t"),
    year = c("2020", "2021"),
    value = c("10", "bad"),
    notes = c(NA_character_, NA_character_),
    footnotes = c(NA_character_, NA_character_),
    yearbook = c("yb", "yb"),
    document = c("good.xlsx", "bad.xlsx")
  )

  fs::file_create(fs::path(config$paths$data$imports$raw, "bad.xlsx"))

  output_dt <- audit_data_output(input_dt, config)

  testthat::expect_true(data.table::is.data.table(output_dt))
  testthat::expect_identical(nrow(output_dt), nrow(input_dt))
  testthat::expect_true(is.double(output_dt$value))
  testthat::expect_true(is.na(output_dt$value[[2]]))
  testthat::expect_true(all(names(input_dt) %in% names(output_dt)))

  testthat::expect_error(
    audit_data_output(dataset_dt = input_dt, config = list()),
    class = "rlang_error"
  )
})
