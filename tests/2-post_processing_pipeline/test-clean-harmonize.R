# tests/2-post_processing_pipeline/test-clean-harmonize.R
# integration tests for scripts/2-post_processing_pipeline/22-clean_harmonize_data.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
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
source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "22-clean_harmonize_data.R"
  ),
  echo = FALSE
)


# helpers for creating rule files
create_clean_rule_file <- function(
  config,
  rules_df,
  filename = "clean_rules_test.csv"
) {
  readr::write_csv(
    rules_df,
    file.path(config$paths$data$imports$cleaning, filename)
  )
}

create_harmonize_rule_file <- function(
  config,
  rules_df,
  filename = "harmonize_rules_test.csv"
) {
  readr::write_csv(
    rules_df,
    file.path(config$paths$data$imports$harmonization, filename)
  )
}


# --- run_cleaning_layer_batch ------------------------------------------------

testthat::test_that("run_cleaning_layer_batch applies clean rules", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(config, clean_rules)

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(result$unit[[1]], "kilogram")
  testthat::expect_equal(result$unit[[2]], "kg")

  audit <- attr(result, "layer_audit")
  testthat::expect_true(nrow(audit) >= 1L)
})


# --- run_harmonize_layer_batch -----------------------------------------------

testthat::test_that("run_harmonize_layer_batch applies harmonize rules", {
  config <- build_test_config()

  harmonize_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "variable",
    value_target_raw = "Prod",
    value_target = "Production",
    stringsAsFactors = FALSE
  )
  create_harmonize_rule_file(config, harmonize_rules)

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    variable = c("Prod", "Prod"),
    stringsAsFactors = FALSE
  )

  result <- run_harmonize_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(result$variable[[1]], "Production")
  testthat::expect_equal(result$variable[[2]], "Prod")
})


# --- clean then harmonize integration ----------------------------------------

testthat::test_that("clean and harmonize pipeline applies both stages sequentially", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(config, clean_rules)

  harmonize_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "variable",
    value_target_raw = "Prod",
    value_target = "Production",
    stringsAsFactors = FALSE
  )
  create_harmonize_rule_file(config, harmonize_rules)

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    variable = c("Prod", "Prod"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  cleaned <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )
  harmonized <- run_harmonize_layer_batch(
    dataset_dt = cleaned,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(harmonized$unit[[1]], "kilogram")
  testthat::expect_equal(harmonized$variable[[1]], "Production")
  testthat::expect_equal(harmonized$variable[[2]], "Prod")
})


# --- auto-create missing columns ---------------------------------------------

testthat::test_that("clean layer auto-creates missing rule-referenced columns", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "source_missing",
    value_source_raw = "match_me",
    column_target = "target_missing",
    value_target_raw = "before",
    value_target = "after",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(config, clean_rules)

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_true("source_missing" %in% names(result))
  testthat::expect_true("target_missing" %in% names(result))
  testthat::expect_true(all(is.na(result$source_missing)))
})


# --- source rewrite ----------------------------------------------------------

testthat::test_that("clean stage applies optional source rewrites", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source = "Wheat cleaned",
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(config, clean_rules)

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(result$product[[1]], "Wheat cleaned")
  testthat::expect_equal(result$product[[2]], "Rice")
  testthat::expect_equal(result$unit[[1]], "kilogram")
})


# --- blank source rewrite → NA -----------------------------------------------

testthat::test_that("clean stage blank source rewrite assigns NA on matched rows", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "continent",
    value_source_raw = "asia",
    value_source = "",
    column_target = "product",
    value_target_raw = "wheat",
    value_target = "asia wheat",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(config, clean_rules)

  input_dt <- data.frame(
    continent = c("asia", "europe"),
    product = c("wheat", "wheat"),
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_true(is.na(result$continent[[1]]))
  testthat::expect_equal(result$continent[[2]], "europe")
  testthat::expect_equal(result$product[[1]], "asia wheat")
})


# --- same column source/target precedence ------------------------------------

testthat::test_that("when source and target columns are identical target rewrite has precedence", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source = "Wheat source",
    column_target = "product",
    value_target_raw = "Wheat",
    value_target = "Wheat target",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(config, clean_rules)

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(result$product[[1]], "Wheat target")
  testthat::expect_equal(result$product[[2]], "Rice")
})


# --- harmonize source rewrite ------------------------------------------------

testthat::test_that("harmonize stage applies optional source rewrites", {
  config <- build_test_config()

  harmonize_rules <- data.frame(
    column_source = "variable",
    value_source_raw = "Prod",
    value_source = "Production",
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram",
    stringsAsFactors = FALSE
  )
  create_harmonize_rule_file(config, harmonize_rules)

  input_dt <- data.frame(
    variable = c("Prod", "Import"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  result <- run_harmonize_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(result$variable[[1]], "Production")
  testthat::expect_equal(result$variable[[2]], "Import")
  testthat::expect_equal(result$unit[[1]], "kilogram")
})


# --- no rules scenario -------------------------------------------------------

testthat::test_that("run_cleaning_layer_batch returns unchanged data with no rules", {
  config <- build_test_config()

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(result$product[[1]], "Wheat")
  testthat::expect_equal(result$unit[[1]], "kg")
})
