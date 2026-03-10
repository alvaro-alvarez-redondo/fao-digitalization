# script: dependency script
# description: defines the project dependency registry and helper functions to
# validate, install, and load required packages for the analysis pipeline.
# this restores a clear script-level title while preserving roxygen-only
# documentation

required_packages <- c(
  "checkmate",
  "cli",
  "data.table",
  "dplyr",
  "fs",
  "future",
  "future.apply",
  "here",
  "openxlsx",
  "progressr",
  "purrr",
  "readr",
  "readxl",
  "renv",
  "stringi",
  "stringr",
  "tibble",
  "tidyr",
  "tidyselect"
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

  return(invisible(TRUE))
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

  return(missing_packages)
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

  return(invisible(NULL))
}


#' @title collect namespaced dependencies
#' @description scan project scripts for namespace-qualified calls (`pkg::fn`) and
#' return the unique package names referenced.
#' @param scripts_root character scalar path to the script root directory.
#' defaults to `here::here("scripts")`.
#' @return character vector of unique package names discovered in script files.
#' @importFrom checkmate check_string check_directory_exists
#' @importFrom fs dir_ls
#' @examples
#' \dontrun{
#' collect_namespaced_dependencies()
#' }
collect_namespaced_dependencies <- function(
  scripts_root = here::here("scripts")
) {
  abort_on_checkmate_failure(checkmate::check_string(
    scripts_root,
    min.chars = 1
  ))
  abort_on_checkmate_failure(checkmate::check_directory_exists(scripts_root))

  script_files <- fs::dir_ls(
    path = scripts_root,
    recurse = TRUE,
    type = "file",
    glob = "*.R"
  )

  package_names <- script_files |>
    unname() |>
    lapply(function(script_file) {
      script_lines <- readLines(script_file, warn = FALSE, encoding = "UTF-8")
      matches <- regmatches(
        script_lines,
        gregexpr("[A-Za-z][A-Za-z0-9.]*::", script_lines, perl = TRUE)
      )

      package_candidates <- unlist(matches, use.names = FALSE)

      if (length(package_candidates) == 0) {
        return(character(0))
      }

      return(sub("::$", "", package_candidates))
    }) |>
    unlist(use.names = FALSE) |>
    unique() |>
    sort()

  return(package_names)
}

#' @title audit dependency registry
#' @description compare the declared dependency registry to namespaced package
#' usage found in project scripts.
#' @param packages declared package registry.
#' @param scripts_root character scalar path to the script root directory.
#' @return named list with `declared`, `used`, `unused`, and `missing` package
#' vectors.
#' @importFrom checkmate check_character check_string check_directory_exists
#' @examples
#' \dontrun{
#' audit_dependency_registry(required_packages)
#' }
audit_dependency_registry <- function(
  packages = required_packages,
  scripts_root = here::here("scripts")
) {
  abort_on_checkmate_failure(checkmate::check_character(
    packages,
    any.missing = FALSE,
    min.len = 1
  ))
  abort_on_checkmate_failure(checkmate::check_string(
    scripts_root,
    min.chars = 1
  ))
  abort_on_checkmate_failure(checkmate::check_directory_exists(scripts_root))

  declared_packages <- sort(unique(packages))
  used_packages <- collect_namespaced_dependencies(scripts_root = scripts_root)

  dependency_audit <- list(
    declared = declared_packages,
    used = used_packages,
    unused = setdiff(declared_packages, used_packages),
    missing = setdiff(used_packages, declared_packages)
  )

  return(dependency_audit)
}
