options(
  fao.run_pipeline.auto = FALSE,
  fao.run_general_pipeline.auto = FALSE,
  fao.run_import_pipeline.auto = FALSE,
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_export_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "01-setup.R"), echo = FALSE)
source(here::here("scripts", "run_pipeline.R"), echo = FALSE)

testthat::test_that("resolve_pipeline_files returns deterministic stage order", {
  pipeline_root <- file.path(tempdir(), "pipeline-root")
  dir.create(pipeline_root, recursive = TRUE)

  pipeline_files <- resolve_pipeline_files(pipeline_root)

  testthat::expect_identical(
    basename(pipeline_files),
    get_expected_stage_runner_names()
  )
})

testthat::test_that("validate_stage_runner_names errors for invalid length", {
  testthat::expect_error(
    validate_stage_runner_names(c("run_general_pipeline.R", "run_import_pipeline.R")),
    "must have length of exactly 4"
  )
})

testthat::test_that("validate_stage_runner_names errors for out-of-order stage runners", {
  testthat::expect_error(
    validate_stage_runner_names(c(
      "run_import_pipeline.R",
      "run_general_pipeline.R",
      "run_post_processing_pipeline.R",
      "run_export_pipeline.R"
    )),
    "invalid pipeline stage runner configuration"
  )
})
