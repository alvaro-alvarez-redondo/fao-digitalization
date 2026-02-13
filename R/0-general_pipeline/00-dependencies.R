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
  "renv",
  "testthat",
  "checkmate",
  "cli",
  "progress"
)

#' @title check dependencies
#' @description validates a character vector of package names, identifies missing packages,
#' and installs any package that is not currently available in the active r library paths.
#' this function is defensive and installs packages silently when required.
#' @param packages character vector. must be non-missing, non-empty, and contain at least
#' one package name.
#' @return character vector of missing package names. returns an empty character vector when
#' all dependencies are already installed.
#' @importFrom checkmate assert_character
#' @importFrom utils install.packages installed.packages
#' @examples
#' missing_packages <- check_dependencies(c("stats", "utils"))
#' missing_packages
check_dependencies <- function(packages) {
  checkmate::assert_character(packages, any.missing = FALSE, min.len = 1)

  missing_packages <- setdiff(packages, rownames(utils::installed.packages()))

  if (length(missing_packages) > 0) {
    utils::install.packages(missing_packages)
  }

  missing_packages
}

#' @title load dependencies
#' @description validates a character vector of package names and loads each package with
#' startup messages suppressed to keep project logs clean and deterministic.
#' @param packages character vector. must be non-missing, non-empty, and contain at least
#' one package name.
#' @return invisible null. used for side effects by attaching packages to the session.
#' @importFrom checkmate assert_character
#' @importFrom purrr walk
#' @importFrom base suppressPackageStartupMessages library
#' @examples
#' load_dependencies(c("stats", "utils"))
load_dependencies <- function(packages) {
  checkmate::assert_character(packages, any.missing = FALSE, min.len = 1)

  packages |>
    purrr::walk(function(package_name) {
      suppressPackageStartupMessages(
        library(package_name, character.only = TRUE)
      )
    })

  invisible(NULL)
}
