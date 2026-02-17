source(here::here("R/0-general_pipeline/00-dependencies.R"), echo = FALSE)

testthat::test_that("check_dependencies returns empty output for installed packages", {
  missing_packages <- check_dependencies("stats")

  testthat::expect_identical(missing_packages, character(0))
})

testthat::test_that("load_dependencies validates inputs with cli errors", {
  testthat::expect_error(
    load_dependencies(character()),
    class = "rlang_error"
  )
})

testthat::test_that("check_dependencies validates inputs with cli errors", {
  testthat::expect_error(
    check_dependencies(character()),
    class = "rlang_error"
  )
})
