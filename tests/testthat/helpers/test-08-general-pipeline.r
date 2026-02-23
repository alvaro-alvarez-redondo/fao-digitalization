testthat::local_options(fao.run_general_pipeline.auto = FALSE)
source(here::here("R/0-general_pipeline/run_general_pipeline.R"), echo = FALSE)

testthat::test_that("source_general_scripts returns sourced paths", {
  mock_source <- function(file, echo = FALSE) {
    invisible(file)
  }

  mock_assert_file_exists <- function(file, access = "r", extension = NULL) {
    invisible(TRUE)
  }

  sourced_paths <- testthat::with_mocked_bindings(
    source = mock_source,
    `checkmate::assert_file_exists` = mock_assert_file_exists,
    {
      source_general_scripts(c("00-dependencies.R", "01-setup.R"))
    }
  )

  testthat::expect_type(sourced_paths, "character")
  testthat::expect_true(all(grepl("R/0-general_pipeline", sourced_paths, fixed = TRUE)))
  testthat::expect_identical(
    basename(sourced_paths),
    c("00-dependencies.R", "01-setup.R")
  )
})

testthat::test_that("source_general_scripts fails for missing script", {
  testthat::expect_error(
    source_general_scripts("missing-script.R")
  )
})

testthat::test_that("run_general_pipeline keeps backward-compatible default", {
  run_general_pipeline_formals <- formals(run_general_pipeline)

  testthat::expect_true(identical(
    run_general_pipeline_formals$dataset_name,
    "fao_data_raw"
  ))
})
