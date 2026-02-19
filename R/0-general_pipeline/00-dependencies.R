# script: dependency script
# description: defines the project dependency registry and helper functions to
# validate, install, and load required packages for the analysis pipeline.
# this restores a clear script-level title while preserving roxygen-only
# documentation

required_packages <- c(
  "tidyverse",
  "data.table",
  "readxl",
  "janitor",
  "openxlsx",
  "fs",
  "here",
  "stringi",
  "progressr",
  "renv",
  "testthat",
  "checkmate",
  "cli",
  "progress"
)

#' @title abort on failed checkmate checks
#' @description convert a `checkmate::check_*` result into a cli abort when validation fails.
#' this keeps user-facing errors consistent and structured.
#' @param check_result logical true or character scalar returned by a `checkmate::check_*`
#' validator.
#' @return invisible true when validation passes.
#' @importFrom checkmate assert check_true check_string
#' @importFrom cli cli_abort
#' @examples
#' abort_on_checkmate_failure(checkmate::check_true(TRUE))
abort_on_checkmate_failure <- function(check_result) {
  checkmate::assert(
    checkmate::check_true(check_result),
    checkmate::check_string(check_result, min.chars = 1)
  )

  if (!isTRUE(check_result)) {
    cli::cli_abort(check_result)
  }

  invisible(TRUE)
}

#' @title check dependencies
#' @description validates a character vector of package names, identifies missing packages,
#' and installs any package that is not currently available via namespace lookup.
#' this function is defensive and installs packages silently when required.
#' @param packages character vector. must be non-missing, non-empty, and contain at least
#' one package name.
#' @return character vector of missing package names. returns an empty character vector when
#' all dependencies are already installed.
#' @importFrom checkmate check_character
#' @importFrom base requireNamespace
#' @importFrom cli cli_alert_info cli_warn
#' @importFrom renv install
#' @examples
#' missing_packages <- check_dependencies(c("stats", "utils"))
#' missing_packages
check_dependencies <- function(packages) {
  abort_on_checkmate_failure(checkmate::check_character(
    packages,
    any.missing = FALSE,
    min.len = 1
  ))

  package_availability <- vapply(
    packages,
    FUN = requireNamespace,
    FUN.VALUE = logical(1),
    quietly = TRUE
  )

  missing_packages <- unique(packages[!package_availability])

  if (length(missing_packages) > 0) {
    cli::cli_warn(c(
      "installing missing dependencies with renv",
      "i" = "missing packages: {toString(missing_packages)}"
    ))

    renv::install(missing_packages)

    cli::cli_alert_info("dependency installation completed")
  }

  missing_packages
}

#' @title load dependencies
#' @description validates a character vector of package names and attaches each package with
#' startup messages suppressed to keep project logs clean and deterministic.
#' @param packages character vector. must be non-missing, non-empty, and contain at least
#' one package name.
#' @return invisible null. used for side effects by attaching packages to the session.
#' @importFrom checkmate check_character
#' @importFrom purrr walk
#' @importFrom cli cli_abort
#' @importFrom base require
#' @examples
#' load_dependencies(c("stats", "utils"))
load_dependencies <- function(packages) {
  abort_on_checkmate_failure(checkmate::check_character(
    packages,
    any.missing = FALSE,
    min.len = 1
  ))

  packages |>
    purrr::walk(function(package_name) {
      package_loaded <- suppressPackageStartupMessages(
        require(package_name, character.only = TRUE, quietly = TRUE)
      )

      if (!isTRUE(package_loaded)) {
        cli::cli_abort("failed to attach package `{package_name}`.")
      }
    })

  invisible(NULL)
}
