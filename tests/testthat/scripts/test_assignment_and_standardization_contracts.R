options(
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "23-standardize_units.R"), echo = FALSE)
source(here::here("scripts", "run_pipeline.R"), echo = FALSE)

testthat::test_that("assign_environment_values assigns named values deterministically", {
  env <- new.env(parent = emptyenv())

  result <- assign_environment_values(
    values = list(alpha = 1L, beta = "b"),
    env = env
  )

  testthat::expect_true(isTRUE(invisible(result)))
  testthat::expect_identical(env$alpha, 1L)
  testthat::expect_identical(env$beta, "b")
})

testthat::test_that("assign_environment_values handles empty lists and overwrites existing bindings", {
  env <- new.env(parent = emptyenv())
  env$alpha <- 100L

  empty_result <- assign_environment_values(values = list(), env = env)
  overwrite_result <- assign_environment_values(values = list(alpha = 5L), env = env)

  testthat::expect_true(isTRUE(invisible(empty_result)))
  testthat::expect_true(isTRUE(invisible(overwrite_result)))
  testthat::expect_identical(env$alpha, 5L)
})

testthat::test_that("assign_environment_values errors on unnamed values", {
  env <- new.env(parent = emptyenv())

  testthat::expect_error(
    assign_environment_values(values = list(1L, 2L), env = env),
    "named"
  )
})

testthat::test_that("validate_conversion_rules accepts normalized alias schema", {
  alias_rules <- data.table::data.table(
    product = c("wheat", "rice"),
    from_unit = c("kg", "kg"),
    to_unit = c("g", "g"),
    factor = c("1000", "1000"),
    offset = c("0", "0")
  )

  normalized_rules <- normalize_conversion_rule_columns(alias_rules)

  testthat::expect_invisible(validate_conversion_rules(normalized_rules))
})

testthat::test_that("validate_conversion_rules errors on duplicated product/source_unit", {
  duplicate_rules <- data.table::data.table(
    product = c("wheat", "wheat"),
    source_unit = c("kg", "kg"),
    target_unit = c("g", "mg"),
    multiplier = c(1000, 1000000),
    addend = c(0, 0)
  )

  testthat::expect_error(
    validate_conversion_rules(duplicate_rules),
    "duplicate"
  )
})

testthat::test_that("apply_standardize_rules converts values and stabilizes output contract", {
  mapped_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    value = c("2", "3")
  )

  prepared_rules_dt <- prepare_standardize_rules(data.table::data.table(
    product = "wheat",
    source_unit = "kg",
    target_unit = "g",
    multiplier = 1000,
    addend = 0
  ))

  result <- apply_standardize_rules(
    mapped_dt = mapped_dt,
    prepared_rules_dt = prepared_rules_dt,
    unit_column = "unit",
    value_column = "value",
    product_column = "product"
  )

  testthat::expect_named(result, c("data", "matched_count", "unmatched_count"))
  testthat::expect_s3_class(result$data, "data.table")
  testthat::expect_identical(result$matched_count, 1L)
  testthat::expect_identical(result$unmatched_count, 1L)
  testthat::expect_identical(result$data$unit[[1]], "g")
  testthat::expect_equal(result$data$value[[1]], 2000)
  testthat::expect_equal(result$data$value[[2]], 3)
})

testthat::test_that("apply_standardize_rules zero-rule path is deterministic", {
  mapped_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", ""),
    value = c("2", "")
  )

  result <- apply_standardize_rules(
    mapped_dt = mapped_dt,
    prepared_rules_dt = data.table::data.table(),
    unit_column = "unit",
    value_column = "value",
    product_column = "product"
  )

  testthat::expect_identical(result$matched_count, 0L)
  testthat::expect_identical(result$unmatched_count, 1L)
  testthat::expect_true(is.na(result$data$value[[2]]))
})

testthat::test_that("apply_standardize_rules errors for non-numeric value payload", {
  mapped_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg",
    value = "not_numeric"
  )

  testthat::expect_error(
    apply_standardize_rules(
      mapped_dt = mapped_dt,
      prepared_rules_dt = data.table::data.table(),
      unit_column = "unit",
      value_column = "value",
      product_column = "product"
    ),
    "non-numeric"
  )
})

testthat::test_that("run_pipeline validates missing pipeline roots", {
  testthat::expect_error(
    run_pipeline(show_view = FALSE, pipeline_root = "this/path/does/not/exist"),
    "pipeline root does not exist"
  )
})
