# tests/2-post_processing_pipeline/test-clean-harmonize.R
# integration tests for R/2-post_processing_pipeline/22-clean_harmonize_data.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here(
    "r",
    "2-post_processing_pipeline",
    "21-post_processing_utilities.R"
  ),
  echo = FALSE
)
source(
  here::here(
    "r",
    "2-post_processing_pipeline",
    "23-post_processing_rule_engine.R"
  ),
  echo = FALSE
)
source(
  here::here(
    "r",
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
  testthat::expect_true("loop" %in% names(audit))
  testthat::expect_true(all(audit$loop >= 1L))
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


# --- multi-pass convergence and safeguards -----------------------------------

testthat::test_that("harmonize multi-pass converges chained footnote and unit rules without duplicate rule rows", {
  config <- build_test_config()

  harmonize_rules <- data.frame(
    column_source = c("footnotes", "unit"),
    value_source_raw = c("number", "quintals"),
    value_source = c("number", "quintal"),
    column_target = c("unit", "unit"),
    value_target_raw = c("quintal", "quintals"),
    value_target = c("count", "quintal"),
    stringsAsFactors = FALSE
  )
  create_harmonize_rule_file(
    config = config,
    rules_df = harmonize_rules,
    filename = "harmonize_rules_multipass_convergence.csv"
  )

  input_dt <- data.frame(
    footnotes = "number",
    unit = "quintals",
    stringsAsFactors = FALSE
  )

  result <- run_harmonize_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  diagnostics <- attr(result, "layer_diagnostics")

  testthat::expect_equal(result$unit[[1]], "count")
  testthat::expect_true(is.list(diagnostics$multi_pass))
  testthat::expect_true(diagnostics$multi_pass$converged)
  testthat::expect_true(diagnostics$multi_pass$passes_executed >= 2L)
})

testthat::test_that("clean multi-pass detects deterministic two-state cycle with warn policy", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = c("unit", "unit"),
    value_source_raw = c("a", "b"),
    value_source = c("b", "a"),
    column_target = c("unit", "unit"),
    value_target_raw = c("a", "b"),
    value_target = c("b", "a"),
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(
    config = config,
    rules_df = clean_rules,
    filename = "clean_rules_cycle_warn.csv"
  )

  input_dt <- data.frame(
    unit = "a",
    stringsAsFactors = FALSE
  )

  result <- testthat::expect_warning(
    run_cleaning_layer_batch(
      dataset_dt = input_dt,
      config = config,
      dataset_name = "demo"
    ),
    regexp = "cycle detected"
  )

  diagnostics <- attr(result, "layer_diagnostics")

  testthat::expect_true(diagnostics$multi_pass$cycle_detected)
  testthat::expect_identical(
    diagnostics$multi_pass$stop_reason,
    "cycle_detected"
  )
  testthat::expect_true(diagnostics$multi_pass$passes_executed >= 2L)
})

testthat::test_that("clean multi-pass aborts on cycle when cycle_policy is abort", {
  config <- build_test_config()
  config$post_processing <- list(
    multi_pass = list(cycle_policy = "abort")
  )

  clean_rules <- data.frame(
    column_source = c("unit", "unit"),
    value_source_raw = c("a", "b"),
    value_source = c("b", "a"),
    column_target = c("unit", "unit"),
    value_target_raw = c("a", "b"),
    value_target = c("b", "a"),
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(
    config = config,
    rules_df = clean_rules,
    filename = "clean_rules_cycle_abort.csv"
  )

  input_dt <- data.frame(
    unit = "a",
    stringsAsFactors = FALSE
  )

  testthat::expect_error(
    run_cleaning_layer_batch(
      dataset_dt = input_dt,
      config = config,
      dataset_name = "demo"
    ),
    regexp = "cycle detected"
  )
})

testthat::test_that("clean multi-pass reports max_passes reached before convergence", {
  config <- build_test_config()
  config$post_processing <- list(
    multi_pass = list(
      enabled_by_stage = c(clean = TRUE, harmonize = TRUE),
      max_passes_by_stage = c(clean = 1L, harmonize = 10L)
    )
  )

  clean_rules <- data.frame(
    column_source = c("unit", "unit"),
    value_source_raw = c("quintals", "quintal"),
    value_source = c("quintal", NA_character_),
    column_target = c("unit", "notes"),
    value_target_raw = c("quintals", NA_character_),
    value_target = c("quintal", "normalized"),
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(
    config = config,
    rules_df = clean_rules,
    filename = "clean_rules_max_passes.csv"
  )

  input_dt <- data.frame(
    unit = "quintals",
    notes = NA_character_,
    stringsAsFactors = FALSE
  )

  result <- testthat::expect_warning(
    run_cleaning_layer_batch(
      dataset_dt = input_dt,
      config = config,
      dataset_name = "demo"
    ),
    regexp = "max_passes=1"
  )

  diagnostics <- attr(result, "layer_diagnostics")

  testthat::expect_equal(result$unit[[1]], "quintal")
  testthat::expect_true(is.na(result$notes[[1]]))
  testthat::expect_true(
    diagnostics$multi_pass$max_passes_reached_before_convergence
  )
  testthat::expect_identical(
    diagnostics$multi_pass$stop_reason,
    "max_passes_reached"
  )
})

testthat::test_that("clean multi-pass treats net-zero-change pass as convergence, not cycle", {
  config <- build_test_config()

  clean_rules_first <- data.frame(
    column_source = "unit",
    value_source_raw = "a",
    value_source = "b",
    column_target = "unit",
    value_target_raw = "a",
    value_target = "b",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(
    config = config,
    rules_df = clean_rules_first,
    filename = "clean_rules_cancel_step_01.csv"
  )

  clean_rules_second <- data.frame(
    column_source = "unit",
    value_source_raw = "b",
    value_source = "a",
    column_target = "unit",
    value_target_raw = "b",
    value_target = "a",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(
    config = config,
    rules_df = clean_rules_second,
    filename = "clean_rules_cancel_step_02.csv"
  )

  input_dt <- data.frame(
    unit = "a",
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  diagnostics <- attr(result, "layer_diagnostics")

  testthat::expect_equal(result$unit[[1]], "a")
  testthat::expect_true(diagnostics$multi_pass$converged)
  testthat::expect_false(diagnostics$multi_pass$cycle_detected)
  testthat::expect_identical(
    diagnostics$multi_pass$stop_reason,
    "converged_zero_change"
  )
})

testthat::test_that("clean footnote rewrites are audited only on effective change loops", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "footnotes",
    value_source_raw = "australian mandate",
    value_source = "__australian mandate__",
    column_target = "footnotes",
    value_target_raw = "australian mandate",
    value_target = "__australian mandate__",
    stringsAsFactors = FALSE
  )
  create_clean_rule_file(
    config = config,
    rules_df = clean_rules,
    filename = "clean_footnotes_loop_audit.csv"
  )

  input_dt <- data.frame(
    footnotes = "australian mandate",
    stringsAsFactors = FALSE
  )

  result <- run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  audit_dt <- attr(result, "layer_audit")

  testthat::expect_equal(result$footnotes[[1]], "__australian mandate__")
  testthat::expect_identical(sort(unique(audit_dt$loop)), 1L)
})

testthat::test_that("clean stage persists runtime cache artifact deterministically", {
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

  invisible(run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  ))

  runtime_cache_settings <- resolve_stage_runtime_cache_settings(config)
  cache_file_path <- build_stage_runtime_cache_file_path(
    config = config,
    runtime_cache_settings = runtime_cache_settings
  )

  testthat::expect_true(file.exists(cache_file_path))

  cache_entries <- read_stage_runtime_cache_entries(
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings
  )
  testthat::expect_true(length(cache_entries) >= 1L)
  testthat::expect_true(any(startsWith(names(cache_entries), "clean::")))
})

testthat::test_that("clean stage payload bundle can be reloaded from disk cache", {
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

  invisible(run_cleaning_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  ))

  first_bundle <- get_cached_stage_payload_bundle(
    config = config,
    stage_name = "clean"
  )
  runtime_cache_settings <- resolve_stage_runtime_cache_settings(config)
  cache_file_path <- build_stage_runtime_cache_file_path(
    config = config,
    runtime_cache_settings = runtime_cache_settings
  )

  rm(
    list = ls(.stage_payload_bundle_cache, all.names = TRUE),
    envir = .stage_payload_bundle_cache
  )

  disk_bundle <- load_stage_payload_bundle_from_disk(
    cache_file_path = cache_file_path,
    runtime_cache_settings = runtime_cache_settings,
    cache_key = first_bundle$cache_key
  )

  testthat::expect_true(is.list(disk_bundle))
  testthat::expect_identical(disk_bundle$cache_key, first_bundle$cache_key)
  testthat::expect_true(length(disk_bundle$canonical_payloads) >= 1L)
})

testthat::test_that("stage payload cache key changes when rule file contents change", {
  config <- build_test_config()

  clean_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram",
    stringsAsFactors = FALSE
  )
  rule_file_name <- "clean_cache_key_test.csv"
  create_clean_rule_file(config, clean_rules, filename = rule_file_name)

  key_before <- build_stage_payload_cache_key(
    config = config,
    stage_name = "clean"
  )

  updated_rules <- rbind(
    clean_rules,
    data.frame(
      column_source = "product",
      value_source_raw = "Rice",
      column_target = "unit",
      value_target_raw = "kg",
      value_target = "gram",
      stringsAsFactors = FALSE
    )
  )
  create_clean_rule_file(config, updated_rules, filename = rule_file_name)

  key_after <- build_stage_payload_cache_key(
    config = config,
    stage_name = "clean"
  )

  testthat::expect_false(identical(key_before, key_after))
})

testthat::test_that("harmonize stage inherits missing stage controls from defaults when config overrides are partial", {
  config <- build_test_config()
  config$post_processing <- list(
    multi_pass = list(
      max_passes_by_stage = c(clean = 2L)
    )
  )

  harmonize_rules <- data.frame(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram",
    stringsAsFactors = FALSE
  )
  create_harmonize_rule_file(
    config = config,
    rules_df = harmonize_rules,
    filename = "harmonize_rules_partial_override.csv"
  )

  input_dt <- data.frame(
    product = "Wheat",
    unit = "kg",
    stringsAsFactors = FALSE
  )

  result <- run_harmonize_layer_batch(
    dataset_dt = input_dt,
    config = config,
    dataset_name = "demo"
  )

  diagnostics <- attr(result, "layer_diagnostics")
  expected_harmonize_max_passes <- get_pipeline_constants()$post_processing$multi_pass$max_passes_by_stage[[
    "harmonize"
  ]]

  testthat::expect_equal(result$unit[[1]], "kilogram")
  testthat::expect_equal(
    diagnostics$multi_pass$max_passes,
    expected_harmonize_max_passes
  )
})
