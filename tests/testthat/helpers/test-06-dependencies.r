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

testthat::test_that("load_dependencies returns invisible null for valid packages", {
  loaded_packages <- load_dependencies("stats")

  testthat::expect_null(loaded_packages)
})

testthat::test_that("load_dependencies aborts for unknown packages", {
  testthat::expect_error(
    load_dependencies("package_that_does_not_exist"),
    class = "rlang_error"
  )
})

testthat::test_that("load_dependencies keeps backward-compatible return type", {
  testthat::expect_identical(
    load_dependencies("utils"),
    invisible(NULL)
  )
})

testthat::test_that("check_dependencies validates inputs with cli errors", {
  testthat::expect_error(
    check_dependencies(character()),
    class = "rlang_error"
  )
})
