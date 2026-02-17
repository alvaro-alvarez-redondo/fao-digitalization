source(here::here("R/1-import_pipeline/14-data_audit.R"), echo = FALSE)

test_that("audit_character_non_empty_column returns findings for missing and blank rows", {
  dataset_dt <- data.frame(
    continent = c("asia", "", "   ", NA_character_),
    stringsAsFactors = FALSE
  )

  output_dt <- audit_character_non_empty_column(dataset_dt, "continent")

  expect_s3_class(output_dt, "data.table")
  expect_equal(output_dt$row_index, c(2L, 3L, 4L))
  expect_true(all(output_dt$audit_type == "character_non_empty"))
})

test_that("audit_character_non_empty_column returns empty findings for valid data", {
  dataset_dt <- data.frame(continent = c("asia", "africa"), stringsAsFactors = FALSE)

  output_dt <- audit_character_non_empty_column(dataset_dt, "continent")

  expect_equal(nrow(output_dt), 0)
})

test_that("audit_numeric_string_column validates only digits and one decimal point", {
  dataset_dt <- data.frame(
    value = c("10", "10.5", "10.5.2", "10a", " 10", NA_character_),
    stringsAsFactors = FALSE
  )

  output_dt <- audit_numeric_string_column(dataset_dt, "value")

  expect_equal(output_dt$row_index, c(3L, 4L, 5L, 6L))
  expect_true(all(output_dt$audit_type == "numeric_string"))
})

test_that("audit_numeric_string_column aborts when configured column is missing", {
  dataset_dt <- data.frame(other = c("1", "2"), stringsAsFactors = FALSE)

  expect_error(
    audit_numeric_string_column(dataset_dt, "value"),
    class = "rlang_error"
  )
})

test_that("identify_audit_errors remains backward compatible with legacy config", {
  config <- load_pipeline_config("fao_data_raw")

  dataset_dt <- data.frame(
    continent = c("asia", ""),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("tonnes", "tonnes"),
    year = c("2020", "2020"),
    value = c("10", "10a"),
    notes = c(NA_character_, NA_character_),
    footnotes = c(NA_character_, NA_character_),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("clean.xlsx", "dirty.xlsx"),
    stringsAsFactors = FALSE
  )

  audit_dt <- identify_audit_errors(dataset_dt, config)

  expect_equal(nrow(audit_dt), 1)
  expect_equal(audit_dt$document[[1]], "dirty.xlsx")
})

test_that("run_audit_by_type supports config-driven audit column types", {
  config <- load_pipeline_config("fao_data_raw")
  config$audit_columns_by_type <- list(
    character_non_empty = c("continent", "country"),
    numeric_string = "value"
  )

  dataset_dt <- data.frame(
    continent = c("asia", ""),
    country = c("nepal", "nepal"),
    product = c("rice", "rice"),
    variable = c("production", "production"),
    unit = c("tonnes", "tonnes"),
    year = c("2020", "2020"),
    value = c("10", "10a"),
    notes = c(NA_character_, NA_character_),
    footnotes = c(NA_character_, NA_character_),
    yearbook = c("yb_2020", "yb_2020"),
    document = c("clean.xlsx", "dirty.xlsx"),
    stringsAsFactors = FALSE
  )

  audit_result <- run_audit_by_type(dataset_dt, config)

  expect_true(is.list(audit_result))
  expect_named(audit_result, c("findings", "invalid_row_index"))
  expect_equal(audit_result$invalid_row_index, 2L)
  expect_true(all(c("character_non_empty", "numeric_string") %in% audit_result$findings$audit_type))
})
