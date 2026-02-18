source(here::here("R/0-general_pipeline/00-dependencies.R"), echo = FALSE)

testthat::test_that("check_dependencies returns empty output when all packages are available", {
  mock_require_namespace <- function(package, quietly = TRUE) {
    TRUE
  }

  missing_packages <- testthat::with_mocked_bindings(
    requireNamespace = mock_require_namespace,
    .env = environment(check_dependencies),
    {
      check_dependencies(c("stats", "utils"))
    }
  )

  testthat::expect_identical(missing_packages, character(0))
})

testthat::test_that("check_dependencies returns unique missing packages", {
  mock_require_namespace <- function(package, quietly = TRUE) {
    package == "stats"
  }

  mock_renv_install <- function(packages) {
    invisible(packages)
  }

  missing_packages <- testthat::with_mocked_bindings(
    requireNamespace = mock_require_namespace,
    `renv::install` = mock_renv_install,
    .env = environment(check_dependencies),
    {
      check_dependencies(c("pkg_a", "pkg_a", "stats", "pkg_b"))
    }
  )

  testthat::expect_identical(missing_packages, c("pkg_a", "pkg_b"))
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

testthat::test_that("check_dependencies keeps backward-compatible return type", {
  mock_require_namespace <- function(package, quietly = TRUE) {
    package != "pkg_missing"
  }

  mock_renv_install <- function(packages) {
    invisible(packages)
  }

  missing_packages <- testthat::with_mocked_bindings(
    requireNamespace = mock_require_namespace,
    `renv::install` = mock_renv_install,
    .env = environment(check_dependencies),
    {
      check_dependencies(c("stats", "pkg_missing"))
    }
  )

  testthat::expect_type(missing_packages, "character")
  testthat::expect_identical(missing_packages, c("pkg_missing"))
})
