source(here::here("tests/testthat/setup/test-00-context.r"), echo = FALSE)

testthat::test_dir(
  here::here("tests/testthat/data_integrity"),
  reporter = "summary"
)

testthat::test_dir(
  here::here("tests/testthat/transformations"),
  reporter = "summary"
)

testthat::test_dir(
  here::here("tests/testthat/edge_cases"),
  reporter = "summary"
)
