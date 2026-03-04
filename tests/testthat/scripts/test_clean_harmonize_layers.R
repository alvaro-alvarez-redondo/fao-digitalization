options(
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_clean_harmonize_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21-post_processing_utilities.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "22-clean_harmonize_data.R"), echo = FALSE)

testthat::test_that("clean and harmonize layers apply deterministic rule payloads", {
  root_dir <- tempfile("fao-clean-harmonize-")
  dir.create(root_dir, recursive = TRUE)

  clean_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  harmonize_dir <- file.path(root_dir, "data", "1-import", "13-harmonize_imports")
  dir.create(clean_dir, recursive = TRUE)
  dir.create(harmonize_dir, recursive = TRUE)

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "unit",
    value_target_raw = "kg",
    value_target_clean = "kilogram",
    stringsAsFactors = FALSE
  )

  harmonize_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "variable",
    value_target_raw = "Prod",
    value_target_harmonize = "Production",
    stringsAsFactors = FALSE
  )

  readr::write_csv(clean_rules, file.path(clean_dir, "clean_rules_product.csv"))
  readr::write_csv(harmonize_rules, file.path(harmonize_dir, "harmonize_rules_variable.csv"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = clean_dir,
          harmonization = harmonize_dir
        )
      )
    )
  )

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    variable = c("Prod", "Prod"),
    unit = c("kg", "kg"),
    value = c("10", "20"),
    stringsAsFactors = FALSE
  )

  cleaned_dt <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  harmonized_dt <- run_harmonize_layer_batch(
    dataset_dt = cleaned_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(cleaned_dt$unit[[1]], "kilogram")
  testthat::expect_equal(cleaned_dt$unit[[2]], "kg")

  testthat::expect_equal(harmonized_dt$variable[[1]], "Production")
  testthat::expect_equal(harmonized_dt$variable[[2]], "Prod")

  clean_audit <- attr(cleaned_dt, "layer_audit")
  harmonize_audit <- attr(harmonized_dt, "layer_audit")

  testthat::expect_true(nrow(clean_audit) >= 1)
  testthat::expect_true(nrow(harmonize_audit) >= 1)
})


testthat::test_that("stage-prefixed schemas are accepted by coerce_rule_schema", {
  raw_rule_dt <- data.frame(
    clean_column_source = "product",
    clean_value_source_raw = "Wheat",
    clean_column_target = "unit",
    clean_value_target_raw = "kg",
    clean_value_target_clean = "kilogram",
    stringsAsFactors = FALSE
  )

  canonical_dt <- coerce_rule_schema(
    rule_dt = raw_rule_dt,
    stage_name = "clean",
    rule_file_id = "clean_rules_stage_prefixed.xlsx"
  )

  testthat::expect_true(all(
    c(
      "column_source",
      "value_source_raw",
      "value_source_clean",
      "column_target",
      "value_target_raw",
      "value_target_clean"
    ) %in% colnames(canonical_dt)
  ))

  testthat::expect_equal(canonical_dt$value_target_clean[[1]], "kilogram")
  testthat::expect_true("value_source_clean" %in% colnames(canonical_dt))
  testthat::expect_true(is.na(canonical_dt$value_source_clean[[1]]))
})


testthat::test_that("clean layer auto-creates missing rule-referenced columns", {
  root_dir <- tempfile("fao-clean-missing-cols-")
  dir.create(root_dir, recursive = TRUE)

  clean_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  harmonize_dir <- file.path(root_dir, "data", "1-import", "13-harmonize_imports")
  dir.create(clean_dir, recursive = TRUE)
  dir.create(harmonize_dir, recursive = TRUE)

  clean_rules <- data.frame(
    column_source = "source_missing",
    value_source_raw = "match_me",
    column_target = "target_missing",
    value_target_raw = "before",
    value_target_clean = "after",
    stringsAsFactors = FALSE
  )

  readr::write_csv(clean_rules, file.path(clean_dir, "clean_rules_missing_columns.csv"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = clean_dir,
          harmonization = harmonize_dir
        )
      )
    )
  )

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    variable = c("Prod", "Prod"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  cleaned_dt <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_true("source_missing" %in% colnames(cleaned_dt))
  testthat::expect_true("target_missing" %in% colnames(cleaned_dt))
  testthat::expect_true(all(is.na(cleaned_dt$source_missing)))
  testthat::expect_true(all(is.na(cleaned_dt$target_missing)))
})


testthat::test_that("clean stage applies optional source rewrites", {
  root_dir <- tempfile("fao-clean-source-rewrite-")
  dir.create(root_dir, recursive = TRUE)

  clean_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  harmonize_dir <- file.path(root_dir, "data", "1-import", "13-harmonize_imports")
  dir.create(clean_dir, recursive = TRUE)
  dir.create(harmonize_dir, recursive = TRUE)

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source_clean = "Wheat cleaned",
    column_target = "unit",
    value_target_raw = "kg",
    value_target_clean = "kilogram",
    stringsAsFactors = FALSE
  )

  readr::write_csv(clean_rules, file.path(clean_dir, "clean_rules_source_rewrite.csv"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = clean_dir,
          harmonization = harmonize_dir
        )
      )
    )
  )

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    stringsAsFactors = FALSE
  )

  cleaned_dt <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(cleaned_dt$product[[1]], "Wheat cleaned")
  testthat::expect_equal(cleaned_dt$product[[2]], "Rice")
  testthat::expect_equal(cleaned_dt$unit[[1]], "kilogram")
})


testthat::test_that("when source and target columns are identical target rewrite has precedence", {
  root_dir <- tempfile("fao-clean-same-column-")
  dir.create(root_dir, recursive = TRUE)

  clean_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  harmonize_dir <- file.path(root_dir, "data", "1-import", "13-harmonize_imports")
  dir.create(clean_dir, recursive = TRUE)
  dir.create(harmonize_dir, recursive = TRUE)

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source_clean = "Wheat source",
    column_target = "product",
    value_target_raw = "Wheat",
    value_target_clean = "Wheat target",
    stringsAsFactors = FALSE
  )

  readr::write_csv(clean_rules, file.path(clean_dir, "clean_rules_same_column.csv"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = clean_dir,
          harmonization = harmonize_dir
        )
      )
    )
  )

  input_dt <- data.frame(
    product = c("Wheat", "Rice"),
    stringsAsFactors = FALSE
  )

  cleaned_dt <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_equal(cleaned_dt$product[[1]], "Wheat target")
  testthat::expect_equal(cleaned_dt$product[[2]], "Rice")
})
