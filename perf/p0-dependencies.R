#' @title Performance dependency module
#' @description Dependency registry and helpers for validating, installing, and
#'   loading packages required by the performance workflow.
#' @keywords internal
#' @noRd
NULL

# ── 0. performance dependencies ───────────────────────────────────────────────

#' @title Required performance packages
#' @description Package registry used by performance scripts.
#' @keywords internal
#' @noRd
perf_required_packages <- c(
  "checkmate",
  "cli",
  "data.table",
  "fs",
  "glue",
  "here",
  "progressr",
  "readxl",
  "readr",
  "renv",
  "stringi"
)

#' @title Validate package vector
#' @description Internal validator for dependency vectors.
#' @param packages Character vector of package names.
#' @return Invisible TRUE when validation passes.
#' @keywords internal
#' @noRd
.validate_perf_package_vector <- function(packages) {
  if (!is.character(packages) || length(packages) == 0L || anyNA(packages)) {
    stop("packages must be a non-empty character vector with no missing values")
  }

  if (any(trimws(packages) == "")) {
    stop("packages must not contain empty package names")
  }

  return(invisible(TRUE))
}

#' @title Abort on failed checkmate checks
#' @description Convert checkmate validation output to a cli abort.
#' @param check_result Logical TRUE or character scalar from checkmate checks.
#' @return Invisible TRUE when validation passes.
abort_on_perf_checkmate_failure <- function(check_result) {
  has_checkmate <- requireNamespace("checkmate", quietly = TRUE)
  has_cli <- requireNamespace("cli", quietly = TRUE)

  if (!has_checkmate || !has_cli) {
    if (!isTRUE(check_result)) {
      stop(as.character(check_result))
    }
    return(invisible(TRUE))
  }

  checkmate::assert(
    checkmate::check_true(check_result),
    checkmate::check_string(check_result, min.chars = 1)
  )

  if (!isTRUE(check_result)) {
    cli::cli_abort(check_result)
  }

  return(invisible(TRUE))
}

#' @title Check performance dependencies
#' @description Validate package names and install missing packages using renv.
#' @param packages Character vector of package names.
#' @return Character vector of packages that were missing before installation.
check_perf_dependencies <- function(packages = perf_required_packages) {
  .validate_perf_package_vector(packages)

  package_availability <- vapply(
    packages,
    FUN = requireNamespace,
    FUN.VALUE = logical(1),
    quietly = TRUE
  )

  missing_packages <- unique(packages[!package_availability])

  if (length(missing_packages) > 0L) {
    if (requireNamespace("cli", quietly = TRUE)) {
      cli::cli_warn(c(
        "installing missing performance dependencies",
        "i" = "missing packages: {toString(missing_packages)}"
      ))
    } else {
      message(sprintf(
        "installing missing performance dependencies: %s",
        toString(missing_packages)
      ))
    }

    if (requireNamespace("renv", quietly = TRUE)) {
      renv::install(missing_packages)
    } else {
      utils::install.packages(missing_packages)
    }

    if (requireNamespace("cli", quietly = TRUE)) {
      cli::cli_alert_info("performance dependency installation completed")
    } else {
      message("performance dependency installation completed")
    }

    still_missing <- vapply(
      missing_packages,
      FUN = requireNamespace,
      FUN.VALUE = logical(1),
      quietly = TRUE
    )

    if (any(!still_missing)) {
      stop(sprintf(
        "failed to install required perf packages: %s",
        toString(missing_packages[!still_missing])
      ))
    }
  }

  return(missing_packages)
}

#' @title Load performance dependencies
#' @description Attach dependencies with startup messages suppressed.
#' @param packages Character vector of package names.
#' @return Invisible NULL.
load_perf_dependencies <- function(packages = perf_required_packages) {
  .validate_perf_package_vector(packages)

  attached_packages <- sub(
    "^package:",
    "",
    grep("^package:", search(), value = TRUE)
  )
  packages_to_load <- setdiff(packages, attached_packages)

  if (length(packages_to_load) == 0L) {
    return(invisible(NULL))
  }

  for (package_name in packages_to_load) {
    package_loaded <- suppressPackageStartupMessages(
      require(package_name, character.only = TRUE, quietly = TRUE)
    )

    if (!isTRUE(package_loaded)) {
      if (requireNamespace("cli", quietly = TRUE)) {
        cli::cli_abort("failed to attach package `{package_name}`.")
      }
      stop(sprintf("failed to attach package '%s'", package_name))
    }
  }

  return(invisible(NULL))
}

#' @title Ensure performance dependencies
#' @description Check/install and then attach performance dependencies.
#' @return Invisible TRUE.
ensure_perf_dependencies <- function() {
  check_perf_dependencies(perf_required_packages)
  load_perf_dependencies(perf_required_packages)
  return(invisible(TRUE))
}
