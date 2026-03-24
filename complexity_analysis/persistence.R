# complexity_analysis/persistence.R
# description: persistence layer for the Big O complexity analysis module.
#   saves the full analysis object to a .qs file using qs::qsave() when the
#   qs package is available. gracefully falls back to emitting a warning and
#   continuing execution when qs is not installed, so JSON and plot outputs
#   are always produced regardless of the persistence outcome.
#
# sourced by: complexity_analysis/run_analysis.R

# ── persistence layer ────────────────────────────────────────────────────────

#' @title save complete analysis object
#' @description persists the full results list to a .qs binary file.
#'   the default output path matches the project-standard location:
#'   `data/2-post_processing/post_processing_diagnostics/complexity_analysis_complete.qs`
#'
#'   if the `qs` package is not installed this function emits a warning and
#'   returns the path unchanged (execution continues normally).
#' @param results named list returned by `run_all_benchmarks()`.
#' @param path character scalar — destination file path. parent directories
#'   are created automatically.
#' @return invisible character scalar — `path` (whether or not the write
#'   succeeded, so callers can propagate the intended path in the output list).
save_analysis_complete <- function(results, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)

  if (!requireNamespace("qs", quietly = TRUE)) {
    warning(
      "Package 'qs' is not installed — skipping .qs persistence. ",
      "JSON and plot outputs are still produced."
    )
    return(invisible(path))
  }

  tryCatch(
    {
      qs::qsave(results, path)
      message(sprintf("  Analysis object saved to: %s", path))
    },
    error = function(e) {
      warning(sprintf(
        "qs::qsave() failed (%s) — skipping .qs persistence. ",
        conditionMessage(e)
      ))
    }
  )

  return(invisible(path))
}

#' @title load complete analysis object
#' @description reads a previously saved .qs analysis file.
#' @param path character scalar — path to the .qs file.
#' @return the deserialized results list, or NULL if the file cannot be read.
load_analysis_complete <- function(path) {
  if (!file.exists(path)) {
    warning(sprintf("analysis file not found: %s", path))
    return(NULL)
  }
  if (!requireNamespace("qs", quietly = TRUE)) {
    warning("Package 'qs' is not installed — cannot load .qs file.")
    return(NULL)
  }
  tryCatch(
    qs::qread(path),
    error = function(e) {
      warning(sprintf("qs::qread() failed: %s", conditionMessage(e)))
      NULL
    }
  )
}
