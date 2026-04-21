source(
  here::here("tests", "testthat", "r", "test_setup_context.R"),
  echo = FALSE
)

testthat::test_dir(
  here::here("tests", "testthat", "r"),
  reporter = "summary"
)
