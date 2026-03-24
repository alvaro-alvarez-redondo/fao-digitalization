# module: run_analysis
# description: master entry point for the WHEP Big O empirical complexity
#   analysis. sources all modular sub-scripts in order and exposes
#   run_big_o_analysis() as the single public entry point.
#
# IMPORTANT: this script is fully independent from the main pipeline. it
#   sources only the general-purpose helper scripts (read-only, no I/O side
#   effects) and generates all inputs synthetically. it never modifies, reads,
#   or writes any production data files, except for persisting analysis results
#   to the designated diagnostics output path.

# prevent any pipeline stage from auto-executing when its runner script is
# loaded transitively
options(
  whep.run_pipeline.auto = FALSE,
  whep.run_general_pipeline.auto = FALSE,
  whep.run_import_pipeline.auto = FALSE,
  whep.run_post_processing_pipeline.auto = FALSE,
  whep.run_export_pipeline.auto = FALSE,
  whep.checkpointing.enabled = FALSE
)

# resolve the project root regardless of working directory
.ca_project_root <- tryCatch(
  here::here(),
  error = function(e) {
    # fallback: assume this script lives one level below the project root
    # (complexity_analysis/run_analysis.R)
    normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
  }
)

.source_pipeline_script <- function(relative_path) {
  abs_path <- file.path(.ca_project_root, "scripts", relative_path)
  if (!file.exists(abs_path)) {
    warning(sprintf("pipeline script not found, skipping: %s", abs_path))
    return(invisible(FALSE))
  }
  tryCatch(
    {
      source(abs_path, echo = FALSE, local = FALSE)
      invisible(TRUE)
    },
    error = function(e) {
      warning(sprintf(
        "failed to source %s: %s",
        relative_path,
        conditionMessage(e)
      ))
      invisible(FALSE)
    }
  )
}

# source the general pipeline helpers (pure functions, no side-effects)
.source_pipeline_script("0-general_pipeline/01-setup.R")
.source_pipeline_script("0-general_pipeline/02-helpers.R")

# source individual pipeline-stage function libraries (no I/O at top level)
.source_pipeline_script("1-import_pipeline/12-transform.R")
.source_pipeline_script("1-import_pipeline/13-validate_log.R")
.source_pipeline_script("1-import_pipeline/15-output.R")
.source_pipeline_script("2-post_processing_pipeline/24-standardize_units.R")
.source_pipeline_script("3-export_pipeline/31-export_lists.R")

# ── source modular complexity-analysis sub-scripts ───────────────────────────

.source_ca_script <- function(filename) {
  abs_path <- file.path(.ca_project_root, "complexity_analysis", filename)
  if (!file.exists(abs_path)) {
    stop(sprintf("complexity analysis module not found: %s", abs_path))
  }
  source(abs_path, echo = FALSE, local = FALSE)
  invisible(TRUE)
}

.source_ca_script("config.R")
.source_ca_script("workload_generators.R")
.source_ca_script("model_fitting.R")
.source_ca_script("timing_engine.R")
.source_ca_script("benchmark_registry.R")
.source_ca_script("aggregation.R")
.source_ca_script("reporting.R")


# ── main entry point ──────────────────────────────────────────────────────────

#' @title run Big O analysis
#' @description top-level entry point. runs the full empirical Big O analysis
#'   for the WHEP pipeline and produces console output, a JSON summary, runtime
#'   plots, and a persistent `.qs` snapshot saved to the post-processing
#'   diagnostics directory. when `quiet` is FALSE a `progressr`
#'   txtprogressbar handler is configured and each benchmark function displays
#'   its own 1/T … T/T progress bar (T = number of input sizes in
#'   `cfg$input_sizes`).
#'
#' @param cfg list. analysis configuration. defaults to `get_big_o_config()`.
#'   override individual fields to change input sizes, reps, output location,
#'   etc.
#' @param output_dir character scalar. directory for JSON and plots. overrides
#'   `cfg$output_dir` when supplied. defaults to `tempdir()/big_o_analysis`.
#' @param write_plots logical. whether to write PNG plots. overrides
#'   `cfg$produce_plots` when supplied.
#' @param quiet logical. suppress progress messages and progress bars.
#'
#' @return named list:
#'   \describe{
#'     \item{raw}{data.table — all timing observations}
#'     \item{summary}{data.table — per-(function, n) statistics}
#'     \item{complexity}{data.table — complexity class per function}
#'     \item{json_path}{character — path of written JSON file}
#'     \item{plot_paths}{character — paths of written PNG files}
#'     \item{qs_path}{character — path of written .qs snapshot}
#'   }
#'
#' @importFrom progressr handlers handler_txtprogressbar
run_big_o_analysis <- function(
  cfg = get_big_o_config(),
  output_dir = NULL,
  write_plots = NULL,
  quiet = FALSE
) {
  if (!is.null(output_dir)) {
    cfg$output_dir <- output_dir
  }
  if (!is.null(write_plots)) {
    cfg$produce_plots <- write_plots
  }
  cfg$quiet <- quiet

  if (is.null(cfg$output_dir)) {
    cfg$output_dir <- file.path(tempdir(), "big_o_analysis")
  }

  start_time <- proc.time()[["elapsed"]]
  if (!quiet) {
    message(strrep("─", 70L))
    message("  WHEP Pipeline — Empirical Big O Analysis")
    message(sprintf(
      "  input_sizes : %s",
      paste(cfg$input_sizes, collapse = ", ")
    ))
    message(sprintf("  n_reps      : %d", cfg$n_reps))
    message(sprintf("  output_dir  : %s", cfg$output_dir))
    message(strrep("─", 70L))
    progressr::handlers(progressr::handler_txtprogressbar(
      style = 3,
      width = 40,
      clear = FALSE
    ))
  }

  benchmarks <- build_benchmark_definitions(cfg)
  results <- run_all_benchmarks(benchmarks, cfg)

  print_complexity_report(results$complexity)

  json_path <- file.path(cfg$output_dir, "big_o_summary.json")
  export_results_json(results, json_path)

  plot_paths <- character(0L)
  if (isTRUE(cfg$produce_plots)) {
    plots_dir <- file.path(cfg$output_dir, "plots")
    plot_paths <- write_complexity_plots(results, plots_dir)
  }

  # ── persist complete results as .qs snapshot ─────────────────────────────
  qs_path <- file.path(
    .ca_project_root,
    "data", "2-post_processing", "post_processing_diagnostics",
    "complexity_analysis_complete.qs"
  )
  dir.create(dirname(qs_path), recursive = TRUE, showWarnings = FALSE)
  qs::qsave(results, qs_path)
  if (!quiet) {
    message(sprintf("  .qs snapshot written to: %s", qs_path))
  }

  elapsed <- proc.time()[["elapsed"]] - start_time

  if (!quiet) {
    hrs  <- as.integer(elapsed) %/% 3600
    mins <- (as.integer(elapsed) %% 3600) %/% 60
    secs <- elapsed %% 60

    if (hrs > 0) {
      time_str <- sprintf("%02d:%02d:%04.1f (hh:mm:ss)", hrs, mins, secs)
    } else if (mins > 0) {
      time_str <- sprintf("%02d:%04.1f (mm:ss)", mins, secs)
    } else {
      time_str <- sprintf("%.1f (ss)", secs)
    }

    message(sprintf("\n  Analysis complete in %s", time_str))
    message(strrep("─", 70L))
  }

  results$json_path  <- json_path
  results$plot_paths <- plot_paths
  results$qs_path    <- qs_path
  return(invisible(results))
}
