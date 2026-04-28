# tests/3-anomaly_pipeline/test-anomaly-pipeline.R
# unit tests for R/3-anomaly_pipeline.

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here("r", "3-anomaly_pipeline", "30-anomaly_objects.R"),
  echo = FALSE
)
source(
  here::here("r", "3-anomaly_pipeline", "31-anomaly_computation.R"),
  echo = FALSE
)
source(
  here::here("r", "3-anomaly_pipeline", "run_anomaly_pipeline.R"),
  echo = FALSE
)

testthat::test_that("run_anomaly_pipeline returns typed analytical objects", {
  config <- build_test_config()

  anomaly_dt <- data.table::data.table(
    hemisphere = rep("north", 8),
    continent = rep("europe", 8),
    country = rep("france", 8),
    product = rep("wheat", 8),
    variable = rep("production", 8),
    unit = rep("tonnes", 8),
    year = as.character(2018:2025),
    yearbook = rep("yb_2025", 8),
    value = c(10, 12, 13, 15, 16, 18, 20, 65)
  )

  bundle <- run_anomaly_pipeline(config = config, harmonize_dt = anomaly_dt)

  testthat::expect_true(inherits(bundle$tendencies, "whep_tendencies"))
  testthat::expect_true(inherits(bundle$outliers, "whep_outliers"))
  testthat::expect_true(inherits(bundle$anomalies, "whep_anomalies"))
  testthat::expect_true(
    inherits(bundle$diagnostics, "whep_anomaly_diagnostics")
  )
  testthat::expect_true("schema_version" %in% names(bundle$tendencies))
})

testthat::test_that("percent change is NA when previous period is zero", {
  config <- build_test_config()

  zero_dt <- data.table::data.table(
    hemisphere = rep("north", 6),
    continent = rep("asia", 6),
    country = rep("japan", 6),
    product = rep("rice", 6),
    variable = rep("yield", 6),
    unit = rep("tonnes", 6),
    year = as.character(2019:2024),
    yearbook = rep("yb_2024", 6),
    value = c(0, 10, 11, 12, 13, 14)
  )

  bundle <- run_anomaly_pipeline(config = config, harmonize_dt = zero_dt)
  tendencies_dt <- bundle$tendencies$data

  flagged_row <- tendencies_dt[`..temporal_key` == "2020|yb_2024"]

  testthat::expect_true(is.na(flagged_row$percent_change))
  testthat::expect_identical(
    flagged_row$change_status,
    "undefined_pct_change_zero_previous"
  )
})


testthat::test_that("diagnostics classify insufficient and undefined cases", {
  config <- build_test_config()

  small_group_dt <- data.table::data.table(
    hemisphere = rep("north", 4),
    continent = rep("asia", 4),
    country = rep("japan", 4),
    product = rep("rice", 4),
    variable = rep("yield", 4),
    unit = rep("tonnes", 4),
    year = as.character(2020:2023),
    yearbook = rep("yb_2024", 4),
    value = c(100, 100, 100, 100)
  )

  bundle <- run_anomaly_pipeline(config = config, harmonize_dt = small_group_dt)
  diagnostics_dt <- bundle$diagnostics$data

  testthat::expect_true(
    diagnostics_dt$insufficient_group_size_count[[1]] >= 1
  )
  testthat::expect_true(
    diagnostics_dt$undefined_pct_change_count[[1]] >= 1
  )
})
