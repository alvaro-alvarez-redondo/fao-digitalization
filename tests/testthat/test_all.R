source(
  here::here("tests", "testthat", "scripts", "test_setup_context.R"),
  echo = FALSE
)

testthat::test_dir(
  here::here("tests", "testthat", "scripts"),
  reporter = "summary"
)
