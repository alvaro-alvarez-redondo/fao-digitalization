source(here::here("tests/testthat/0-general_pipeline/test-00-context.r"), echo = FALSE)

testthat::test_dir(
  here::here("tests/testthat/0-general_pipeline"),
  reporter = "summary"
)

testthat::test_dir(
  here::here("tests/testthat/1-import_pipeline"),
  reporter = "summary"
)

testthat::test_dir(
  here::here("tests/testthat/3-export_pipeline"),
  reporter = "summary"
)

testthat::test_dir(
  here::here("tests/testthat/run_pipeline"),
  reporter = "summary"
)
