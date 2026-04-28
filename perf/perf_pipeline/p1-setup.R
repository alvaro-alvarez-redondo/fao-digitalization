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
    stages = c("0-general", "1-import", "2-postpro", "3-export"),
    top_n_bottlenecks = 3L,
    output_dir = NULL,
    qs_path = file.path(
      "data",
      "2-postpro",
      "postpro_diagnostics",
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
  "2-postpro" = "perf_2-postpro_pipeline.md",
  "3-export" = "perf_3-export_pipeline.md"
)

# Canonical markdown report filename for cross-pipeline summary.
perf_general_summary_report_filename <- "perf_whep-digitalization.md"

#' @title Apply preset label to report filename
#' @description Prefix markdown report filenames with the preset label so
#'   artifacts remain self-identifying even if moved across folders.
#' @param filename A character scalar markdown filename.
#' @param preset_name A character scalar preset label.
#' @return A character scalar markdown filename.
apply_perf_preset_to_report_filename <- function(filename, preset_name) {
  file_value <- as.character(filename[[1L]])
  preset_raw <- if (is.null(preset_name) || length(preset_name) == 0L) {
    NA_character_
  } else {
    as.character(preset_name[[1L]])
  }

  if (is.na(preset_raw) || !nzchar(trimws(preset_raw))) {
    return(file_value)
  }

  preset_label <- .sanitize_perf_preset_name(preset_raw, default = "custom")
  has_md_suffix <- grepl("\\.md$", file_value, ignore.case = TRUE)
  file_stem <- sub("\\.md$", "", file_value, ignore.case = TRUE)

  if (grepl(sprintf("^perf_%s_", preset_label), file_stem)) {
    return(file_value)
  }

  prefixed_stem <- if (grepl("^perf_", file_stem)) {
    sub("^perf_", sprintf("perf_%s_", preset_label), file_stem)
  } else {
    sprintf("perf_%s_%s", preset_label, file_stem)
  }

  if (has_md_suffix) {
    paste0(prefixed_stem, ".md")
  } else {
    prefixed_stem
  }
}

#' @title Get pipeline report file map
#' @description Return the canonical stage-to-markdown filename mapping used by
#'   performance reporting export.
#' @param preset_name Optional preset label used to namespace markdown
#'   filenames.
#' @return A named character vector where names are stage IDs and values are
#'   markdown filenames.
get_perf_pipeline_report_file_map <- function(preset_name = NULL) {
  if (is.null(preset_name) || length(preset_name) == 0L) {
    return(perf_pipeline_report_file_map)
  }

  stats::setNames(
    vapply(
      perf_pipeline_report_file_map,
      apply_perf_preset_to_report_filename,
      character(1),
      preset_name = preset_name
    ),
    names(perf_pipeline_report_file_map)
  )
}

#' @title Get general summary report filename
#' @description Return the canonical markdown filename for the project-level
#'   performance summary.
#' @param preset_name Optional preset label used to namespace markdown
#'   filename.
#' @return A character scalar markdown filename.
get_perf_general_summary_report_filename <- function(preset_name = NULL) {
  if (is.null(preset_name) || length(preset_name) == 0L) {
    return(perf_general_summary_report_filename)
  }

  apply_perf_preset_to_report_filename(
    perf_general_summary_report_filename,
    preset_name = preset_name
  )
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

#' @title Sanitize preset label
#' @description Convert an arbitrary preset label into a filesystem-safe,
#'   deterministic slug.
#' @param preset_name A scalar preset label.
#' @param default A fallback label when preset_name is missing or invalid.
#' @return A character scalar safe label.
.sanitize_perf_preset_name <- function(
  preset_name,
  default = "custom"
) {
  if (is.null(preset_name) || length(preset_name) == 0L) {
    return(default)
  }

  label <- trimws(as.character(preset_name[[1L]]))
  if (!nzchar(label) || is.na(label)) {
    return(default)
  }

  safe_label <- tolower(gsub("[^A-Za-z0-9_-]+", "_", label))
  safe_label <- gsub("^_+|_+$", "", safe_label)

  if (!nzchar(safe_label)) {
    return(default)
  }

  safe_label
}

#' @title Build preset output subdirectory label
#' @description Return deterministic subdirectory name for a given preset.
#' @param preset_name A scalar preset label.
#' @return A character scalar subdirectory name.
get_perf_preset_output_subdir <- function(preset_name) {
  paste0(
    "perf_diagnosis_",
    .sanitize_perf_preset_name(preset_name, default = "custom")
  )
}

#' @title Resolve output directory for preset
#' @description Ensure preset runs are exported under a preset-specific
#'   subdirectory to prevent report overwrites between presets.
#' @param output_dir A scalar directory path.
#' @param preset_name A scalar preset label.
#' @return A character scalar output directory path.
resolve_perf_output_dir_for_preset <- function(output_dir, preset_name) {
  if (is.null(output_dir) || length(output_dir) == 0L) {
    base_output_dir <- here::here("perf", "perf_diagnosis")
  } else {
    base_output_dir <- as.character(output_dir[[1L]])
  }

  if (!nzchar(base_output_dir) || is.na(base_output_dir)) {
    base_output_dir <- here::here("perf", "perf_diagnosis")
  }

  preset_subdir <- get_perf_preset_output_subdir(preset_name)
  if (identical(basename(base_output_dir), preset_subdir)) {
    return(base_output_dir)
  }

  file.path(base_output_dir, preset_subdir)
}

#' @title Infer preset name from run config
#' @description Attempt to infer a preset label from explicit config values
#'   and output directory naming conventions.
#' @param cfg A named list run configuration.
#' @param default Fallback label when no preset can be inferred.
#' @return A character scalar preset label.
infer_perf_preset_name <- function(cfg, default = "custom") {
  if (!is.list(cfg)) {
    return(default)
  }

  output_dir <- cfg$output_dir
  if (!is.null(output_dir) && length(output_dir) > 0L) {
    output_basename <- basename(as.character(output_dir[[1L]]))
    output_match <- regexec("^perf_diagnosis_(.+)$", output_basename)
    output_parts <- regmatches(output_basename, output_match)[[1L]]
    if (length(output_parts) >= 2L && nzchar(output_parts[[2L]])) {
      return(.sanitize_perf_preset_name(output_parts[[2L]], default = default))
    }
  }

  presets <- get_perf_run_presets()
  preset_names <- names(presets)

  matches <- vapply(
    preset_names,
    function(name) {
      preset_cfg <- presets[[name]]

      checks <- c(
        identical(
          as.integer(cfg$input_sizes),
          as.integer(preset_cfg$input_sizes)
        ),
        identical(as.integer(cfg$n_reps), as.integer(preset_cfg$n_reps)),
        identical(
          as.integer(cfg$n_year_cols),
          as.integer(preset_cfg$n_year_cols)
        ),
        identical(as.integer(cfg$rng_seed), as.integer(preset_cfg$rng_seed)),
        isTRUE(all.equal(
          as.numeric(cfg$na_fraction),
          as.numeric(preset_cfg$na_fraction)
        )),
        isTRUE(all.equal(
          as.numeric(cfg$dup_fraction),
          as.numeric(preset_cfg$dup_fraction)
        ))
      )

      if (!is.null(preset_cfg$stages)) {
        checks <- c(
          checks,
          identical(as.character(cfg$stages), as.character(preset_cfg$stages))
        )
      }

      all(checks)
    },
    logical(1)
  )

  matched <- preset_names[matches]
  if (length(matched) == 1L) {
    return(.sanitize_perf_preset_name(matched[[1L]], default = default))
  }

  default
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
  merged_cfg <- if (!is.list(overrides) || length(overrides) == 0L) {
    base_cfg
  } else {
    utils::modifyList(base_cfg, overrides)
  }

  merged_cfg$preset_name <- .sanitize_perf_preset_name(
    preset_name,
    default = "custom"
  )
  merged_cfg$output_dir <- resolve_perf_output_dir_for_preset(
    output_dir = merged_cfg$output_dir,
    preset_name = merged_cfg$preset_name
  )

  merged_cfg
}

# ── 3. perf setup helpers (reserved for future expansion) ──
# (Add perf-specific setup helpers here as needed)
