options(
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_clean_harmonize_pipeline.auto = FALSE
)

source(
  here::here("scripts", "0-general_pipeline", "02-helpers.R"),
  echo = FALSE
)
source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "21-post_processing_utilities.R"
  ),
  echo = FALSE
)
source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "23-post_processing_rule_engine.R"
  ),
  echo = FALSE
)

testthat::test_that("validate_canonical_rules allows NA in value columns for clean stage", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    variable = c("Prod", "Prod"),
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
      rule_file_id = "clean_rules_template.xlsx",
      stage_name = "clean"
    )
  )
})

testthat::test_that("validate_canonical_rules remains fail-fast for structural required columns", {
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
      rule_file_id = "clean_rules_template.xlsx",
      stage_name = "clean"
    ),
    regexp = "missing values in required columns"
  )
})

testthat::test_that("apply_conditional_rule_group matches NA keys deterministically", {
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
    rule_file_id = "clean_rules_template.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$unit[[1]], "unknown_unit")
  testthat::expect_equal(result$data$unit[[2]], "kg")
  testthat::expect_equal(result$audit$affected_rows[[1]], 1L)
})


testthat::test_that("validate_canonical_rules allows NA in value columns for harmonize stage", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    variable = c("Prod", "Prod")
  )

  rules_dt <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c(NA_character_, "Rice"),
    column_target = c("variable", "variable"),
    value_target_raw = c(NA_character_, "Prod"),
    value_target_harmonize = c(NA_character_, "Production")
  )

  testthat::expect_invisible(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "harmonize_rules_template.xlsx",
      stage_name = "harmonize"
    )
  )
})


testthat::test_that("empty target clean value is applied as NA_character_", {
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
    rule_file_id = "clean_rules_template.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.na(result$data$unit[[1]]))
  testthat::expect_equal(result$data$unit[[2]], "kg")
  testthat::expect_equal(result$audit$affected_rows[[1]], 1L)
  testthat::expect_true(is.na(result$audit$value_target_result[[1]]))
})
