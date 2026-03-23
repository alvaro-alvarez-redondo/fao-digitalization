# script: run_complexity_analysis
# description: master entry point for the WHEP Big O empirical complexity
#   analysis. sources all modular sub-scripts in order and exposes
#   run_big_o_analysis() as the single public entry point.
#
# IMPORTANT: this script is fully independent from the main pipeline. it
#   sources only the general-purpose helper scripts (read-only, no I/O side
#   effects) and generates all inputs synthetically. it never modifies, reads,
#   or writes any production data files.
#
# usage:
#   source("scripts/complexity_analysis/run_complexity_analysis.R")
#   results <- run_big_o_analysis()              # full analysis
#   results <- run_big_o_analysis(output_dir = "/tmp/big_o_out")  # custom dir
#
# outputs:
#   - console table with complexity class per function and dominant stage
#   - JSON summary file  (<output_dir>/big_o_summary.json)
#   - per-function runtime plots  (<output_dir>/plots/)
#
# file map:
#   91-config.R               — analysis configuration  (get_big_o_config)
#   92-synthetic_data.R       — synthetic data generators
#   93-complexity_models.R    — complexity model fitting (fit_complexity_model)
#   94-timing_harness.R       — timing harness  (run_benchmark, summarise_benchmark)
#   95-benchmark_definitions.R— benchmark catalogue  (build_benchmark_definitions)
#   96-execution_engine.R     — execution loop  (run_all_benchmarks)
#   97-reporting.R            — reporting & plots  (print_complexity_report,
#                               export_results_json, write_complexity_plots)

# ── 0. bootstrap: disable all auto-run options and source helpers ────────────

# prevent any pipeline stage from auto-executing when its runner script is
# loaded transitively
options(
  whep.run_pipeline.auto            = FALSE,
  whep.run_general_pipeline.auto    = FALSE,
  whep.run_import_pipeline.auto     = FALSE,
  whep.run_post_processing_pipeline.auto = FALSE,
  whep.run_export_pipeline.auto     = FALSE,
  whep.checkpointing.enabled        = FALSE
)

# resolve the project root regardless of working directory
.big_o_project_root <- tryCatch(
  here::here(),
  error = function(e) {
    # fallback: assume this script lives two levels below the project root
    # (scripts/complexity_analysis/run_complexity_analysis.R)
    normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
  }
)

.source_pipeline_script <- function(relative_path) {
  abs_path <- file.path(.big_o_project_root, "scripts", relative_path)
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
      warning(sprintf("failed to source %s: %s", relative_path, conditionMessage(e)))
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

.source_complexity_script <- function(filename) {
  abs_path <- file.path(.big_o_project_root,
                        "scripts", "complexity_analysis", filename)
  if (!file.exists(abs_path)) {
    stop(sprintf("complexity analysis module not found: %s", abs_path))
  }
  source(abs_path, echo = FALSE, local = FALSE)
  invisible(TRUE)
}

.source_complexity_script("91-config.R")
.source_complexity_script("92-synthetic_data.R")
.source_complexity_script("93-complexity_models.R")
.source_complexity_script("94-timing_harness.R")
.source_complexity_script("95-benchmark_definitions.R")
.source_complexity_script("96-execution_engine.R")
.source_complexity_script("97-reporting.R")


# ── 8. main entry point ──────────────────────────────────────────────────────

#' @title run Big O analysis
#' @description top-level entry point. runs the full empirical Big O analysis
#'   for the WHEP pipeline and produces console output, a JSON summary, and
#'   runtime plots.
#'
#' @param cfg list. analysis configuration. defaults to `get_big_o_config()`.
#'   override individual fields to change input sizes, reps, output location,
#'   etc.
#' @param output_dir character scalar. directory for JSON and plots. overrides
#'   `cfg$output_dir` when supplied. defaults to `tempdir()/big_o_analysis`.
#' @param write_plots logical. whether to write PNG plots. overrides
#'   `cfg$produce_plots` when supplied.
#' @param quiet logical. suppress progress messages.
#'
#' @return named list:
#'   \describe{
#'     \item{raw}{data.table — all timing observations}
#'     \item{summary}{data.table — per-(function, n) statistics}
#'     \item{complexity}{data.table — complexity class per function}
#'     \item{json_path}{character — path of written JSON file}
#'     \item{plot_paths}{character — paths of written PNG files}
#'   }
#'
#' @examples
#' \dontrun{
#' # run with defaults (outputs to tempdir)
#' results <- run_big_o_analysis()
#'
#' # fast smoke-test with tiny inputs
#' results <- run_big_o_analysis(
#'   cfg = utils::modifyList(get_big_o_config(), list(
#'     input_sizes = c(100L, 500L, 1000L),
#'     n_reps      = 3L
#'   ))
#' )
#'
#' # custom output directory
#' results <- run_big_o_analysis(output_dir = "~/analysis/big_o")
#' }
run_big_o_analysis <- function(
  cfg        = get_big_o_config(),
  output_dir = NULL,
  write_plots = NULL,
  quiet       = FALSE
) {
  if (!is.null(output_dir)) cfg$output_dir    <- output_dir
  if (!is.null(write_plots)) cfg$produce_plots <- write_plots
  cfg$quiet <- quiet

  if (is.null(cfg$output_dir)) {
    cfg$output_dir <- file.path(tempdir(), "big_o_analysis")
  }

  start_time <- proc.time()[["elapsed"]]
  if (!quiet) {
    message(strrep("─", 70L))
    message("  WHEP Pipeline — Empirical Big O Analysis")
    message(sprintf("  input_sizes : %s", paste(cfg$input_sizes, collapse = ", ")))
    message(sprintf("  n_reps      : %d", cfg$n_reps))
    message(sprintf("  output_dir  : %s", cfg$output_dir))
    message(strrep("─", 70L))
  }

  benchmarks <- build_benchmark_definitions(cfg)
  results    <- run_all_benchmarks(benchmarks, cfg)

  print_complexity_report(results$complexity)

  json_path  <- file.path(cfg$output_dir, "big_o_summary.json")
  export_results_json(results, json_path)

  plot_paths <- character(0L)
  if (isTRUE(cfg$produce_plots)) {
    plots_dir  <- file.path(cfg$output_dir, "plots")
    plot_paths <- write_complexity_plots(results, plots_dir)
  }

  elapsed <- proc.time()[["elapsed"]] - start_time
  if (!quiet) {
    message(sprintf("\n  Analysis complete in %.1f s", elapsed))
    message(strrep("─", 70L))
  }

  results$json_path  <- json_path
  results$plot_paths <- plot_paths
  return(invisible(results))
}


# ── auto-run guard ────────────────────────────────────────────────────────────
# run_big_o_analysis() is NOT called automatically when this script is sourced.
# invoke it explicitly:
#
#   results <- run_big_o_analysis()
#
# or with a custom config:
#
#   results <- run_big_o_analysis(
#     cfg = utils::modifyList(get_big_o_config(), list(
#       input_sizes = c(500L, 2000L, 10000L),
#       n_reps      = 10L,
#       output_dir  = "~/big_o_out"
#     ))
#   )
