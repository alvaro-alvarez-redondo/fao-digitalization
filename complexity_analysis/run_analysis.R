# complexity_analysis/run_analysis.R
# description: orchestrator and public entrypoint for the Big O complexity
#   analysis module. sources all sub-modules in dependency order and exposes
#   run_big_o_analysis() as the single public API function.
#
# this script is PURELY a function library — sourcing it has no side effects.
# it does NOT auto-execute any benchmarks. call run_big_o_analysis() explicitly
# to start an analysis run.
#
# module load order (dependency-safe):
#   1. config               — get_big_o_config()
#   2. workload_generators  — make_wide_dt / make_long_dt / make_benchmark_config
#   3. model_fitting        — fit_complexity_model
#   4. timing_engine        — run_benchmark / summarise_benchmark
#   5. aggregation          — add_stage_dominance / get_overall_pipeline_class
#   6. benchmark_registry   — build_benchmark_definitions
#   7. execution_engine     — run_all_benchmarks
#   8. reporting            — print_complexity_report / export_results_json
#   9. persistence          — save_analysis_complete

# ── prevent pipeline stages from auto-executing when sourced transitively ────
options(
  whep.run_pipeline.auto                  = FALSE,
  whep.run_general_pipeline.auto          = FALSE,
  whep.run_import_pipeline.auto           = FALSE,
  whep.run_post_processing_pipeline.auto  = FALSE,
  whep.run_export_pipeline.auto           = FALSE,
  whep.checkpointing.enabled              = FALSE
)

# ── resolve project root ─────────────────────────────────────────────────────
.ca_project_root <- tryCatch(
  here::here(),
  error = function(e) normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
)

.source_script <- function(rel_path, base = .ca_project_root) {
  abs_path <- file.path(base, rel_path)
  if (!file.exists(abs_path)) {
    warning(sprintf("script not found, skipping: %s", abs_path))
    return(invisible(FALSE))
  }
  tryCatch(
    { source(abs_path, echo = FALSE, local = FALSE); invisible(TRUE) },
    error = function(e) {
      warning(sprintf("failed to source %s: %s", rel_path, conditionMessage(e)))
      invisible(FALSE)
    }
  )
}

# ── source pipeline helper scripts (read-only, no side effects) ───────────────
.source_script("scripts/0-general_pipeline/01-setup.R")
.source_script("scripts/0-general_pipeline/02-helpers.R")
.source_script("scripts/1-import_pipeline/12-transform.R")
.source_script("scripts/1-import_pipeline/13-validate_log.R")
.source_script("scripts/1-import_pipeline/15-output.R")
.source_script("scripts/2-post_processing_pipeline/24-standardize_units.R")
.source_script("scripts/3-export_pipeline/31-export_lists.R")

# ── source complexity analysis sub-modules ───────────────────────────────────
.source_script("complexity_analysis/config.R")
.source_script("complexity_analysis/workload_generators.R")
.source_script("complexity_analysis/model_fitting.R")
.source_script("complexity_analysis/timing_engine.R")
.source_script("complexity_analysis/aggregation.R")
.source_script("complexity_analysis/benchmark_registry.R")
.source_script("complexity_analysis/execution_engine.R")
.source_script("complexity_analysis/reporting.R")
.source_script("complexity_analysis/persistence.R")


# ── public entrypoint ────────────────────────────────────────────────────────

#' @title run Big O analysis
#' @description top-level entry point. runs the full empirical Big O analysis
#'   for the WHEP pipeline and produces:
#'   - console complexity report
#'   - JSON summary (always)
#'   - PNG plots (when `write_plots = TRUE`)
#'   - .qs binary of the full results object (when `qs` is available)
#'
#'   the .qs file is saved to:
#'   `data/2-post_processing/post_processing_diagnostics/complexity_analysis_complete.qs`
#'   relative to the project root.
#'
#' @param cfg list. analysis configuration. defaults to `get_big_o_config()`.
#' @param output_dir character scalar. directory for JSON and plots. overrides
#'   `cfg$output_dir` when supplied.
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
#'     \item{qs_path}{character — path of written .qs file}
#'   }
run_big_o_analysis <- function(
  cfg        = get_big_o_config(),
  output_dir = NULL,
  write_plots = NULL,
  quiet      = FALSE
) {
  if (!is.null(output_dir))  cfg$output_dir    <- output_dir
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
    progressr::handlers(progressr::handler_txtprogressbar(
      style = 3, width = 40, clear = FALSE
    ))
  }

  benchmarks <- build_benchmark_definitions(cfg)
  results    <- run_all_benchmarks(benchmarks, cfg)

  print_complexity_report(results$complexity)

  json_path <- file.path(cfg$output_dir, "big_o_summary.json")
  export_results_json(results, json_path)

  plot_paths <- character(0L)
  if (isTRUE(cfg$produce_plots)) {
    plots_dir  <- file.path(cfg$output_dir, "plots")
    plot_paths <- write_complexity_plots(results, plots_dir)
  }

  # persist full results object to .qs
  qs_path <- file.path(
    .ca_project_root,
    "data", "2-post_processing", "post_processing_diagnostics",
    "complexity_analysis_complete.qs"
  )
  save_analysis_complete(results, qs_path)

  elapsed <- proc.time()[["elapsed"]] - start_time
  if (!quiet) {
    hrs  <- as.integer(elapsed) %/% 3600L
    mins <- (as.integer(elapsed) %% 3600L) %/% 60L
    secs <- elapsed %% 60

    time_str <- if (hrs > 0) {
      sprintf("%02d:%02d:%04.1f (hh:mm:ss)", hrs, mins, secs)
    } else if (mins > 0) {
      sprintf("%02d:%04.1f (mm:ss)", mins, secs)
    } else {
      sprintf("%.1f (s)", secs)
    }
    message(sprintf("\n  Analysis complete in %s", time_str))
    message(strrep("─", 70L))
  }

  results$json_path  <- json_path
  results$plot_paths <- plot_paths
  results$qs_path    <- qs_path
  return(invisible(results))
}
