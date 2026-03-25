# module:      run_complexity_analysis
# description: single entry-point for the WHEP pipeline complexity analysis
#   framework. sources all sub-modules in order (ca1 … ca8), loads the
#   pipeline helper functions required by the benchmark closures, and exposes
#   run_big_o_analysis() as the sole public API.
#
# IMPORTANT: this script is fully independent from the main pipeline. it
#   sources only pure-function pipeline scripts (no I/O side effects) and
#   generates all benchmark inputs synthetically. it never reads or writes
#   any production data files — except when persisting the completed analysis
#   object to the configured .qs path.
#
# usage:
#   source(here::here("complexity_analysis", "run_complexity_analysis.R"))
#   result <- run_big_o_analysis()

# ── prevent pipeline auto-execution ─────────────────────────────────────────

options(
  whep.run_pipeline.auto                 = FALSE,
  whep.run_general_pipeline.auto         = FALSE,
  whep.run_import_pipeline.auto          = FALSE,
  whep.run_post_processing_pipeline.auto = FALSE,
  whep.run_export_pipeline.auto          = FALSE,
  whep.checkpointing.enabled             = FALSE
)

# ── resolve project root ─────────────────────────────────────────────────────

.ca_project_root <- tryCatch(
  here::here(),
  error = function(e) {
    normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
  }
)

# ── helpers ───────────────────────────────────────────────────────────────────

.source_pipeline_script <- function(relative_path) {
  abs_path <- file.path(.ca_project_root, "scripts", relative_path)
  if (!file.exists(abs_path)) {
    warning(sprintf("pipeline script not found, skipping: %s", abs_path))
    return(invisible(FALSE))
  }
  tryCatch(
    { source(abs_path, echo = FALSE, local = FALSE); invisible(TRUE) },
    error = function(e) {
      warning(sprintf("failed to source %s: %s", relative_path, conditionMessage(e)))
      invisible(FALSE)
    }
  )
}

.source_ca_script <- function(filename) {
  abs_path <- file.path(.ca_project_root, "complexity_analysis", filename)
  if (!file.exists(abs_path)) {
    stop(sprintf("complexity analysis sub-module not found: %s", abs_path))
  }
  source(abs_path, echo = FALSE, local = FALSE)
  invisible(TRUE)
}

# ── load pipeline helper functions ────────────────────────────────────────────

.source_pipeline_script("0-general_pipeline/01-setup.R")
.source_pipeline_script("0-general_pipeline/02-helpers.R")
.source_pipeline_script("1-import_pipeline/12-transform.R")
.source_pipeline_script("1-import_pipeline/13-validate_log.R")
.source_pipeline_script("1-import_pipeline/15-output.R")
.source_pipeline_script("2-post_processing_pipeline/24-standardize_units.R")
.source_pipeline_script("3-export_pipeline/31-export_lists.R")

# ── load complexity analysis sub-modules (ca1 … ca8) ─────────────────────────

.source_ca_script("ca1-config.R")
.source_ca_script("ca2-synthetic_data.R")
.source_ca_script("ca3-complexity_models.R")
.source_ca_script("ca4-timing_harness.R")
.source_ca_script("ca5-workload_generators.R")
.source_ca_script("ca6-execution_engine.R")
.source_ca_script("ca7-stage_diagnostics.R")
.source_ca_script("ca8-reporting.R")

# ── main entry point ──────────────────────────────────────────────────────────

#' @title run Big O complexity analysis
#' @description top-level entry point for the WHEP pipeline complexity analysis
#'   framework. orchestrates the full analysis: benchmarks all pipeline stages
#'   independently, fits complexity models, builds per-stage and global
#'   diagnostics, prints structured reports, exports a JSON summary, optionally
#'   produces runtime plots, and persists the full analysis object to a .qs file.
#'
#'   stage-level results are available under `result$by_stage[[stage_id]]` and
#'   stage diagnostics under `result$stage_diagnostics[[stage_id]]`.
#'
#' @param cfg named list. analysis configuration. defaults to
#'   `get_analysis_config()`. override specific fields with
#'   `utils::modifyList(get_analysis_config(), list(...))`.
#' @param output_dir character scalar. base directory for JSON and plots.
#'   overrides `cfg$output_dir`. defaults to `tempdir()/complexity_analysis`.
#' @param write_plots logical. whether to produce PNG plots. overrides
#'   `cfg$produce_plots`.
#' @param quiet logical. suppress all progress messages and progress bars.
#' @return named list (invisibly):
#'   \describe{
#'     \item{by_stage}{named list — per-stage raw/summary/complexity results}
#'     \item{raw}{data.table — all timing observations}
#'     \item{summary}{data.table — per-(function, n) statistics}
#'     \item{complexity}{data.table — per-function complexity classes}
#'     \item{stage_diagnostics}{named list — per-stage diagnostic objects}
#'     \item{global_diagnostic}{list — unified pipeline diagnostic}
#'     \item{json_path}{character — path of written JSON file}
#'     \item{plot_paths}{character — paths of written PNG files}
#'     \item{qs_path}{character — path of written .qs file}
#'   }
run_big_o_analysis <- function(
  cfg        = get_analysis_config(),
  output_dir = NULL,
  write_plots = NULL,
  quiet      = FALSE
) {
  if (!is.null(output_dir))  cfg$output_dir   <- output_dir
  if (!is.null(write_plots)) cfg$produce_plots <- write_plots
  cfg$quiet <- quiet

  if (is.null(cfg$output_dir)) {
    cfg$output_dir <- file.path(tempdir(), "complexity_analysis")
  }

  start_time <- proc.time()[["elapsed"]]

  if (!quiet) {
    progressr::handlers(progressr::handler_txtprogressbar(
      style = 3L, width = 40L, clear = FALSE
    ))
    message(strrep("\u2500", 70L))
    message("  WHEP Pipeline \u2014 Complexity Analysis Framework")
    message(sprintf("  stages      : %s", paste(cfg$stages, collapse = ", ")))
    message(sprintf("  input_sizes : %s", paste(cfg$input_sizes, collapse = ", ")))
    message(sprintf("  n_reps      : %d", cfg$n_reps))
    message(sprintf("  output_dir  : %s", cfg$output_dir))
    message(strrep("\u2500", 70L))
  }

  # ── run all stages ─────────────────────────────────────────────────────────
  all_results <- run_all_stages(cfg)

  # ── per-stage diagnostics ─────────────────────────────────────────────────
  stage_diagnostics <- lapply(cfg$stages, function(s) {
    diagnose_stage(s, all_results$by_stage[[s]], cfg)
  })
  names(stage_diagnostics) <- cfg$stages

  # ── print per-stage reports ───────────────────────────────────────────────
  if (!quiet) {
    for (s in cfg$stages) {
      print_stage_report(stage_diagnostics[[s]])
    }
  }

  # ── global diagnostic ─────────────────────────────────────────────────────
  global_diagnostic <- build_global_diagnostic(stage_diagnostics)

  # ── flat report (backward compat) ─────────────────────────────────────────
  if (!quiet) {
    print_global_report(global_diagnostic)
    print_complexity_report(all_results$complexity)
  }

  # ── JSON export ───────────────────────────────────────────────────────────
  json_path <- file.path(cfg$output_dir, "complexity_summary.json")
  export_analysis_json(all_results, json_path)

  # ── plots ─────────────────────────────────────────────────────────────────
  plot_paths <- character(0L)
  if (isTRUE(cfg$produce_plots)) {
    plots_dir  <- file.path(cfg$output_dir, "plots")
    plot_paths <- write_stage_plots(all_results, plots_dir)
  }

  # ── persist full analysis object to .qs ──────────────────────────────────
  analysis_object <- c(
    all_results,
    list(
      stage_diagnostics = stage_diagnostics,
      global_diagnostic = global_diagnostic,
      cfg               = cfg
    )
  )
  qs_out <- persist_analysis(analysis_object, cfg$qs_path)

  elapsed <- proc.time()[["elapsed"]] - start_time
  if (!quiet) {
    time_str <- format_elapsed_time(elapsed)
    message(sprintf("\n  Analysis complete in %s", time_str))
    message(strrep("\u2500", 70L))
  }

  analysis_object$json_path  <- json_path
  analysis_object$plot_paths <- plot_paths
  analysis_object$qs_path    <- qs_out
  return(invisible(analysis_object))
}
