options(
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_clean_harmonize_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21-post_processing_utilities.R"), echo = FALSE)

testthat::test_that("normalize_dataset_column_names is idempotent", {
  dataset_dt <- data.table::data.table(`Côuntry Name` = c("AR", "BR"), `Unit Value` = c("kg", "t"))

  normalize_dataset_column_names(dataset_dt)
  first_names <- colnames(dataset_dt)

  normalize_dataset_column_names(dataset_dt)
  second_names <- colnames(dataset_dt)

  testthat::expect_identical(first_names, second_names)
  testthat::expect_identical(first_names, c("country_name", "unit_value"))
})

testthat::test_that("normalize_rule_columns normalizes accented source and target references", {
  rules_dt <- data.table::data.table(
    column_source = "Côuntry Name",
    value_source_raw = "Argentina",
    column_target = "Unit Value",
    value_target_raw = "kg",
    value_target_clean = "kilogram"
  )

  normalized_rules <- normalize_rule_columns(rules_dt)

  testthat::expect_identical(normalized_rules$column_source[[1]], "country_name")
  testthat::expect_identical(normalized_rules$column_target[[1]], "unit_value")
})

testthat::test_that("ensure_rule_columns_exist creates missing columns as character NA", {
  dataset_dt <- data.table::data.table(product = c("wheat", "rice"))
  rules_dt <- data.table::data.table(
    column_source = "product",
    value_source_raw = c("wheat", "rice"),
    column_target = "unit",
    value_target_raw = c("kg", "kg"),
    value_target_clean = c("kilogram", "kilogram")
  )

  ensure_rule_columns_exist(rules_dt = rules_dt, dataset_dt = dataset_dt)

  testthat::expect_true("unit" %in% colnames(dataset_dt))
  testthat::expect_true(all(is.na(dataset_dt$unit)))
  testthat::expect_type(dataset_dt$unit, "character")
})

testthat::test_that("ensure_rule_columns_exist is idempotent and does not duplicate columns", {
  dataset_dt <- data.table::data.table(product = c("wheat", "rice"), unit = c("kg", "kg"))
  rules_dt <- data.table::data.table(
    column_source = "product",
    value_source_raw = c("wheat", "rice"),
    column_target = "unit",
    value_target_raw = c("kg", "kg"),
    value_target_clean = c("kilogram", "kilogram")
  )

  ensure_rule_columns_exist(rules_dt = rules_dt, dataset_dt = dataset_dt)
  ensure_rule_columns_exist(rules_dt = rules_dt, dataset_dt = dataset_dt)

  testthat::expect_identical(sum(colnames(dataset_dt) == "unit"), 1L)
})

testthat::test_that("validate_canonical_rules creates missing referenced dataset columns", {
  dataset_dt <- data.table::data.table(country_name = c("Argentina"), unit_value = c("kg"))

  rules_dt <- data.table::data.table(
    column_source = "country_name",
    value_source_raw = "Argentina",
    column_target = "missing_target_column",
    value_target_raw = NA_character_,
    value_target_clean = "kilogram"
  )

  testthat::expect_invisible(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "clean_rules.csv",
      stage_name = "clean"
    )
  )

  testthat::expect_true("missing_target_column" %in% colnames(dataset_dt))
  testthat::expect_type(dataset_dt$missing_target_column, "character")
  testthat::expect_true(all(is.na(dataset_dt$missing_target_column)))
})

testthat::test_that("validate_canonical_rules fails clearly for empty column references", {
  dataset_dt <- data.table::data.table(country_name = c("Argentina"), unit_value = c("kg"))

  rules_dt <- data.table::data.table(
    column_source = "",
    value_source_raw = "Argentina",
    column_target = "unit_value",
    value_target_raw = "kg",
    value_target_clean = "kilogram"
  )

  testthat::expect_error(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "clean_rules.csv",
      stage_name = "clean"
    ),
    regexp = "contains empty column references"
  )
})
