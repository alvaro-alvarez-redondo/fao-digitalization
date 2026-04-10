#' @title Performance setup module
#' @description Consolidated setup and configuration helpers for the performance analysis framework.
#'   This module merges configuration, preset, and setup utilities for the perf pipeline.
#' @keywords internal
#' @noRd
NULL

# ── 1. analysis configuration ────────────────────────────────────────────────

#' @title Get default analysis configuration
#' @description Return the default parameter set for a full complexity run.
#'   The output includes input sizing, replication count, stage selection,
#'   output paths, and reporting controls.
#' @return A named list of analysis parameters.
get_analysis_config <- function() {
  list(
    # input sizes (n) to benchmark over — logarithmically spaced so complexity
    # differences are clearly visible across candidate model fits.
    input_sizes = c(100L, 500L, 1000L, 2500L, 5000L, 10000L, 25000L, 50000L),
    n_reps = 5L,
    n_year_cols = 10L,
    excel_fixture_sheet_count = 2L,
    excel_fixture_base_rows_per_sheet = 16L,
    excel_discovery_scale_divisor = 500L,
    excel_discovery_max_workbooks = 96L,
    excel_discovery_repeats_per_iteration = 100L,
    excel_read_workbook_count = 4L,
    excel_read_rows_per_sheet = 128L,
    excel_read_max_sheet_reads_per_iteration = 1000L,
    excel_read_repeats_per_iteration = 3L,
    complexity_r2_tolerance = 0.002,
    complexity_min_best_r2 = 0,
    complexity_min_unique_n = 3L,
    complexity_min_n_span_ratio = 2,
    na_fraction = 0.2,
    dup_fraction = 0.1,
    rng_seed = 42L,
    stages = c("0-general", "1-import", "2-post_processing", "3-export"),
    top_n_bottlenecks = 3L,
    output_dir = NULL,
    qs_path = file.path(
      "data",
      "2-post_processing",
      "post_processing_diagnostics",
      "perf_complete.qs"
    ),
    produce_plots = FALSE,
    quiet = FALSE
  )
}

# Canonical markdown report filenames for each pipeline stage.

# Canonical markdown report filenames for each pipeline stage (prefixed with 'perf_').
perf_pipeline_report_file_map <- c(
  "0-general" = "perf_0-general_pipeline.md",
  "1-import" = "perf_1-import_pipeline.md",
  "2-post_processing" = "perf_2-post_processing_pipeline.md",
  "3-export" = "perf_3-export_pipeline.md"
)

# Canonical markdown report filename for cross-pipeline summary.
perf_general_summary_report_filename <- "perf_whep-digitalization.md"

#' @title Get pipeline report file map
#' @description Return the canonical stage-to-markdown filename mapping used by
#'   performance reporting exports.
#' @return A named character vector where names are stage IDs and values are
#'   markdown filenames.
get_perf_pipeline_report_file_map <- function() {
  perf_pipeline_report_file_map
}

#' @title Get general summary report filename
#' @description Return the canonical markdown filename for the project-level
#'   performance summary.
#' @return A character scalar markdown filename.
get_perf_general_summary_report_filename <- function() {
  perf_general_summary_report_filename
}

# ── 2. user-facing run presets ──────────────────────────────────────────────

#' @title Get performance run presets
#' @description Return hard-coded named presets used by the standalone
#'   performance runner script.
#' @return A named list of preset lists.
get_perf_run_presets <- function() {
  list(
    quick = list(
      input_sizes = as.integer(c(1e3, 1e4)),
      n_reps = 5L,
      n_year_cols = 10L,
      stages = NULL,
      na_fraction = 0.05,
      dup_fraction = 0.02,
      rng_seed = 42L,
      output_dir = here::here("perf", "perf_diagnosis"),
      quiet = FALSE
    ),
    medium = list(
      input_sizes = as.integer(c(1e3, 1e4, 1e5)),
      n_reps = 5L,
      n_year_cols = 20L,
      stages = NULL,
      na_fraction = 0.05,
      dup_fraction = 0.02,
      rng_seed = 42L,
      output_dir = here::here("perf", "perf_diagnosis"),
      quiet = FALSE
    ),
    standard = list(
      input_sizes = as.integer(c(1e3, 1e4, 1e5, 1e6, 1e7)),
      n_reps = 10L,
      n_year_cols = 40L,
      stages = NULL,
      na_fraction = 0.05,
      dup_fraction = 0.02,
      rng_seed = 42L,
      output_dir = here::here("perf", "perf_diagnosis"),
      quiet = FALSE
    ),
    full = list(
      input_sizes = as.integer(c(1e3, 1e4, 1e5, 1e6, 1e7, 1e8)),
      n_reps = 15L,
      n_year_cols = 50L,
      stages = NULL,
      na_fraction = 0.05,
      dup_fraction = 0.02,
      rng_seed = 42L,
      output_dir = here::here("perf", "perf_diagnosis"),
      quiet = FALSE
    )
  )
}

#' @title Build runner configuration from preset
#' @description Retrieve a named preset and optionally apply a list of field
#'   overrides.
#' @param preset_name A character scalar. Must match one of
#'   names(get_perf_run_presets()).
#' @param overrides A named list of field overrides for the selected preset.
#' @return A named list compatible with run_perf().
build_perf_run_config <- function(
  preset_name = "standard",
  overrides = list()
) {
  presets <- get_perf_run_presets()
  if (!preset_name %in% names(presets)) {
    stop(sprintf(
      "unknown preset '%s'. valid presets: %s",
      preset_name,
      paste(names(presets), collapse = ", ")
    ))
  }
  base_cfg <- presets[[preset_name]]
  if (!is.list(overrides) || length(overrides) == 0L) {
    return(base_cfg)
  }
  utils::modifyList(base_cfg, overrides)
}

# ── 3. perf setup helpers (reserved for future expansion) ──
# (Add perf-specific setup helpers here as needed)
