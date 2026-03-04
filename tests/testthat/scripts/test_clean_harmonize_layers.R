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
    value_target_clean = "Production",
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
      "column_target",
      "value_target_raw",
      "value_target_clean"
    ) %in% colnames(canonical_dt)
  ))

  testthat::expect_equal(canonical_dt$value_target_clean[[1]], "kilogram")
})


testthat::test_that("run_cleaning_layer_batch creates missing rule-referenced columns before validation", {
  root_dir <- tempfile("fao-clean-missing-cols-")
  dir.create(root_dir, recursive = TRUE)

  clean_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  dir.create(clean_dir, recursive = TRUE)

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "new_target_column",
    value_target_raw = NA_character_,
    value_target_clean = "created_by_rule",
    stringsAsFactors = FALSE
  )

  readr::write_csv(clean_rules, file.path(clean_dir, "clean_rules_missing_target.csv"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = clean_dir,
          harmonization = file.path(root_dir, "data", "1-import", "13-harmonize_imports")
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

  testthat::expect_true("new_target_column" %in% colnames(cleaned_dt))
  testthat::expect_equal(cleaned_dt$new_target_column[[1]], "created_by_rule")
  testthat::expect_true(is.na(cleaned_dt$new_target_column[[2]]))
})


testthat::test_that("run_harmonize_layer_batch tolerates missing source column by creating it", {
  root_dir <- tempfile("fao-harmonize-missing-source-")
  dir.create(root_dir, recursive = TRUE)

  harmonize_dir <- file.path(root_dir, "data", "1-import", "13-harmonize_imports")
  dir.create(harmonize_dir, recursive = TRUE)

  harmonize_rules <- data.frame(
    column_source = "new_source_column",
    value_source_raw = "trigger",
    column_target = "variable",
    value_target_raw = "Prod",
    value_target_clean = "Production",
    stringsAsFactors = FALSE
  )

  readr::write_csv(
    harmonize_rules,
    file.path(harmonize_dir, "harmonize_rules_missing_source.csv")
  )

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = file.path(root_dir, "data", "1-import", "11-clean_imports"),
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

  harmonized_dt <- run_harmonize_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  testthat::expect_identical(
    names(harmonized_dt)[seq_along(names(input_dt))],
    names(input_dt)
  )
  testthat::expect_true("new_source_column" %in% colnames(harmonized_dt))
  testthat::expect_true(all(is.na(harmonized_dt$new_source_column)))
  testthat::expect_true(all(harmonized_dt$variable == "Prod"))
})
