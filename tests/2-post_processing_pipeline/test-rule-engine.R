# tests/2-post_processing_pipeline/test-rule-engine.R
# unit tests for scripts/2-post_processing_pipeline/21b-post_processing_rule_engine.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21-post_processing_utilities.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21b-post_processing_rule_engine.R"), echo = FALSE)


# --- coerce_rule_schema ------------------------------------------------------

testthat::test_that("coerce_rule_schema normalizes stage-prefixed columns", {
  raw_rule_dt <- data.frame(
    clean_column_source = "product",
    clean_value_source_raw = "Wheat",
    clean_column_target = "unit",
    clean_value_target_raw = "kg",
    clean_value_target_clean = "kilogram",
    stringsAsFactors = FALSE
  )

  result <- coerce_rule_schema(
    rule_dt = raw_rule_dt,
    stage_name = "clean",
    rule_file_id = "test.xlsx"
  )

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true("column_source"      %in% names(result))
  testthat::expect_true("value_target_clean"  %in% names(result))
  testthat::expect_equal(result$value_target_clean[[1]], "kilogram")
})

testthat::test_that("coerce_rule_schema errors on missing required columns", {
  raw_rule_dt <- data.frame(
    column_source = "product",
    stringsAsFactors = FALSE
  )

  testthat::expect_error(
    coerce_rule_schema(
      rule_dt = raw_rule_dt,
      stage_name = "clean",
      rule_file_id = "test.xlsx"
    ),
    "missing required columns"
  )
})

testthat::test_that("coerce_rule_schema errors on duplicate columns after normalization", {
  raw_rule_dt <- data.frame(
    clean_column_source = "product",
    column_source = "product",
    clean_value_source_raw = "Wheat",
    clean_column_target = "unit",
    clean_value_target_raw = "kg",
    clean_value_target_clean = "kilogram",
    stringsAsFactors = FALSE
  )

  testthat::expect_error(
    coerce_rule_schema(
      rule_dt = raw_rule_dt,
      stage_name = "clean",
      rule_file_id = "test.xlsx"
    ),
    "duplicate columns"
  )
})


# --- ensure_rule_referenced_columns ------------------------------------------

testthat::test_that("ensure_rule_referenced_columns adds missing columns to dataset", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  rules_dt <- data.table::data.table(
    column_source = "product",
    column_target = "new_column"
  )

  result <- ensure_rule_referenced_columns(dataset_dt, rules_dt)

  testthat::expect_true("new_column" %in% names(result))
  testthat::expect_true(all(is.na(result$new_column)))
})

testthat::test_that("ensure_rule_referenced_columns preserves existing columns", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  rules_dt <- data.table::data.table(
    column_source = "product",
    column_target = "unit"
  )

  result <- ensure_rule_referenced_columns(dataset_dt, rules_dt)

  testthat::expect_identical(result$unit, c("kg", "kg"))
})


# --- validate_canonical_rules ------------------------------------------------

testthat::test_that("validate_canonical_rules allows NA in value columns for clean stage", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  rules_dt <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c(NA_character_, "Rice"),
    column_target = c("unit", "unit"),
    value_target_raw = c(NA_character_, "kg"),
    value_target_clean = c(NA_character_, "kilogram")
  )

  testthat::expect_invisible(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "test.xlsx",
      stage_name = "clean"
    )
  )
})

testthat::test_that("validate_canonical_rules fails for NA in structural columns", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat"),
    unit = c("kg")
  )

  rules_dt <- data.table::data.table(
    column_source = NA_character_,
    value_source_raw = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target_clean = "kilogram"
  )

  testthat::expect_error(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "test.xlsx",
      stage_name = "clean"
    ),
    "missing values in required columns"
  )
})

testthat::test_that("validate_canonical_rules detects duplicate rule keys", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg"
  )

  rules_dt <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c("Wheat", "Wheat"),
    column_target = c("unit", "unit"),
    value_target_raw = c("kg", "kg"),
    value_target_clean = c("kilogram", "gram")
  )

  testthat::expect_error(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "test.xlsx",
      stage_name = "clean"
    )
  )
})


# --- encode / decode target rule values --------------------------------------

testthat::test_that("encode_target_rule_value replaces empty and NA with placeholder", {
  constants <- get_pipeline_constants()
  result <- encode_target_rule_value(c("value", "", NA_character_))

  testthat::expect_equal(result[1], "value")
  testthat::expect_equal(result[2], constants$na_placeholder)
  testthat::expect_equal(result[3], constants$na_placeholder)
})

testthat::test_that("decode_target_rule_value restores placeholder to NA", {
  constants <- get_pipeline_constants()
  result <- decode_target_rule_value(c("value", constants$na_placeholder))

  testthat::expect_equal(result[1], "value")
  testthat::expect_true(is.na(result[2]))
})


# --- encode_rule_match_key ---------------------------------------------------

testthat::test_that("encode_rule_match_key normalizes values and encodes NA", {
  constants <- get_pipeline_constants()
  result <- encode_rule_match_key(c("Hello World", NA_character_))

  testthat::expect_equal(result[1], "hello world")
  testthat::expect_equal(result[2], constants$na_match_key)
})


# --- apply_conditional_rule_group --------------------------------------------

testthat::test_that("apply_conditional_rule_group applies clean rules", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  group_rules <- data.table::data.table(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "unit",
    value_target_raw = "kg",
    value_target_clean = "kilogram"
  )

  result <- apply_conditional_rule_group(
    dataset_dt = dataset_dt,
    group_rules = group_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_true("data"  %in% names(result))
  testthat::expect_true("audit" %in% names(result))
  testthat::expect_equal(result$data$unit[[1]], "kilogram")
  testthat::expect_equal(result$data$unit[[2]], "kg")
  testthat::expect_true(nrow(result$audit) >= 1L)
})

testthat::test_that("apply_conditional_rule_group matches NA keys", {
  dataset_dt <- data.table::data.table(
    product = c(NA_character_, "Wheat"),
    unit = c(NA_character_, "kg")
  )

  group_rules <- data.table::data.table(
    column_source = "product",
    value_source_raw = NA_character_,
    column_target = "unit",
    value_target_raw = NA_character_,
    value_target_clean = "unknown_unit"
  )

  result <- apply_conditional_rule_group(
    dataset_dt = dataset_dt,
    group_rules = group_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$unit[[1]], "unknown_unit")
  testthat::expect_equal(result$data$unit[[2]], "kg")
  testthat::expect_equal(result$audit$affected_rows[[1]], 1L)
})

testthat::test_that("apply_conditional_rule_group applies empty target as NA", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  group_rules <- data.table::data.table(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "unit",
    value_target_raw = "kg",
    value_target_clean = ""
  )

  result <- apply_conditional_rule_group(
    dataset_dt = dataset_dt,
    group_rules = group_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.na(result$data$unit[[1]]))
  testthat::expect_equal(result$data$unit[[2]], "kg")
})


# --- apply_rule_payload ------------------------------------------------------

testthat::test_that("apply_rule_payload applies multiple rule groups", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    variable = c("Prod", "Prod")
  )

  canonical_rules <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c("Wheat", "Rice"),
    column_target = c("unit", "unit"),
    value_target_raw = c("kg", "kg"),
    value_target_clean = c("kilogram", "gram")
  )

  result <- apply_rule_payload(
    dataset_dt = dataset_dt,
    canonical_rules = canonical_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_equal(result$data$unit[[1]], "kilogram")
  testthat::expect_equal(result$data$unit[[2]], "gram")
  testthat::expect_true(nrow(result$audit) >= 2L)
})

testthat::test_that("apply_rule_payload returns empty audit for zero rules", {
  dataset_dt <- data.table::data.table(product = "Wheat", unit = "kg")

  result <- apply_rule_payload(
    dataset_dt = dataset_dt,
    canonical_rules = data.table::data.table(),
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_equal(nrow(result$audit), 0L)
})
