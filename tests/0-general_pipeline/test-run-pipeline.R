# tests/0-general_pipeline/test-run-pipeline.R
# unit tests for scripts/run_pipeline.R helper utilities

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "run_pipeline.R"), echo = FALSE)


testthat::test_that("format_post_processing_iteration_count handles valid and missing values", {
  testthat::expect_identical(format_post_processing_iteration_count(2L), "2")
  testthat::expect_identical(
    format_post_processing_iteration_count(NA_integer_),
    "N/A"
  )
})

testthat::test_that("extract_post_processing_stage_pass_count reads stage diagnostics", {
  stage_diagnostics <- list(multi_pass = list(passes_executed = 3L))

  testthat::expect_identical(
    extract_post_processing_stage_pass_count(stage_diagnostics),
    3L
  )
  testthat::expect_true(is.na(extract_post_processing_stage_pass_count(list())))
})

testthat::test_that("get_post_processing_iteration_loop_counts returns N/A counts when object is absent", {
  isolated_env <- new.env(parent = emptyenv())

  loop_counts <- get_post_processing_iteration_loop_counts(env = isolated_env)

  testthat::expect_true(is.na(loop_counts$clean))
  testthat::expect_true(is.na(loop_counts$harmonize))
})

testthat::test_that("get_post_processing_iteration_loop_counts reads clean and harmonize pass counts", {
  isolated_env <- new.env(parent = emptyenv())
  pipeline_constants <- get_pipeline_constants()
  harmonized_name <- pipeline_constants$object_names$harmonized

  harmonized_dt <- data.table::data.table(row_id = 1L)
  attr(harmonized_dt, "pipeline_diagnostics") <- list(
    clean = list(multi_pass = list(passes_executed = 2L)),
    harmonize = list(multi_pass = list(passes_executed = 4L))
  )
  assign(harmonized_name, harmonized_dt, envir = isolated_env)

  loop_counts <- get_post_processing_iteration_loop_counts(env = isolated_env)

  testthat::expect_identical(loop_counts$clean, 2L)
  testthat::expect_identical(loop_counts$harmonize, 4L)
})

testthat::test_that("build_post_processing_iteration_summary formats deterministic suffix", {
  isolated_env <- new.env(parent = emptyenv())
  pipeline_constants <- get_pipeline_constants()
  harmonized_name <- pipeline_constants$object_names$harmonized

  harmonized_dt <- data.table::data.table(row_id = 1L)
  attr(harmonized_dt, "pipeline_diagnostics") <- list(
    clean = list(multi_pass = list(passes_executed = 1L)),
    harmonize = list(multi_pass = list(passes_executed = 5L))
  )
  assign(harmonized_name, harmonized_dt, envir = isolated_env)

  summary_suffix <- build_post_processing_iteration_summary(env = isolated_env)

  testthat::expect_identical(
    summary_suffix,
    " | clean loops: 1 | harmonize loops: 5"
  )
})