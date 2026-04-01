#' @title Workload generator module
#' @description Benchmark descriptor builders organized by pipeline stage.
#'   Each descriptor contains metadata and a function factory for one benchmark.
#' @keywords internal
#' @noRd
NULL

# ── 5. workload generators ────────────────────────────────────────────────────
#
# each benchmark descriptor is a named list with:
#   name        : unique identifier (used for reporting and plot file names)
#   stage       : pipeline stage label (one of "0-general", "1-import",
#                 "2-post_processing", "3-export")
#   description : one-line summary of what is measured
#   fn_factory  : function(n) → function() that runs the target operation once
#
# fn_factory must be self-contained (capture everything via closure).

#' @title Resolve integer benchmark config field
#' @description Return an integer scalar from optional config values, with a
#'   deterministic fallback.
#' @param x Candidate scalar value.
#' @param fallback Integer scalar fallback.
#' @return Integer scalar.
#' @keywords internal
#' @noRd
.resolve_integer_cfg <- function(x, fallback) {
  x_int <- suppressWarnings(as.integer(x))
  if (length(x_int) != 1L || is.na(x_int)) {
    return(as.integer(fallback))
  }
  return(as.integer(x_int))
}

#' @title Scale Excel read workload
#' @description Map benchmark input size to a bounded, deterministic number of
#'   Excel reads per timed invocation.
#' @param n Integer scalar benchmark input size.
#' @param max_reads Integer scalar upper bound.
#' @return Integer scalar number of reads per invocation.
#' @keywords internal
#' @noRd
.scale_excel_read_iterations <- function(n, max_reads) {
  safe_n <- max(1L, suppressWarnings(as.integer(n)))
  reads <- as.integer(floor(log10(max(10L, safe_n))))
  return(max(1L, min(max_reads, reads)))
}

#' @title Resolve import folder for Excel benchmark
#' @description Prefer the configured raw-import folder when it contains xlsx
#'   files; otherwise build deterministic fallback fixtures from a readxl sample
#'   workbook.
#' @param cfg A named list from get_analysis_config().
#' @param fixture_copy_count Integer scalar number of fallback fixture copies.
#' @return Character scalar path to a folder containing xlsx files.
#' @keywords internal
#' @noRd
.resolve_excel_benchmark_import_folder <- function(cfg, fixture_copy_count) {
  candidate_folder <- NULL

  if (
    !is.null(cfg$paths) &&
      !is.null(cfg$paths$data) &&
      !is.null(cfg$paths$data$imports) &&
      !is.null(cfg$paths$data$imports$raw)
  ) {
    candidate_folder <- as.character(cfg$paths$data$imports$raw)
  } else if (exists("load_pipeline_config", mode = "function")) {
    pipeline_cfg <- tryCatch(
      load_pipeline_config(),
      error = function(e) NULL
    )
    if (
      !is.null(pipeline_cfg) &&
        !is.null(pipeline_cfg$paths) &&
        !is.null(pipeline_cfg$paths$data) &&
        !is.null(pipeline_cfg$paths$data$imports) &&
        !is.null(pipeline_cfg$paths$data$imports$raw)
    ) {
      candidate_folder <- as.character(pipeline_cfg$paths$data$imports$raw)
    }
  }

  has_candidate_files <- FALSE
  if (!is.null(candidate_folder) && dir.exists(candidate_folder)) {
    candidate_files <- fs::dir_ls(
      path = candidate_folder,
      recurse = TRUE,
      type = "file",
      glob = "*.xlsx"
    )
    has_candidate_files <- length(candidate_files) > 0L
  }

  if (isTRUE(has_candidate_files)) {
    return(candidate_folder)
  }

  sample_file <- readxl::readxl_example("datasets.xlsx")
  if (!nzchar(sample_file) || !file.exists(sample_file)) {
    stop("unable to resolve fallback xlsx fixture for benchmark")
  }

  fixture_dir <- file.path(tempdir(), "perf_excel_fixture")
  dir.create(fixture_dir, recursive = TRUE, showWarnings = FALSE)

  n_copies <- max(1L, as.integer(fixture_copy_count))
  fixture_paths <- file.path(
    fixture_dir,
    sprintf("perf_fixture_%02d.xlsx", seq_len(n_copies))
  )
  copied <- file.copy(sample_file, fixture_paths, overwrite = TRUE)
  if (!all(copied)) {
    stop("failed to create fallback xlsx fixture files")
  }

  return(fixture_dir)
}

#' @title Build read benchmark config
#' @description Build a config object compatible with discover/read import
#'   helpers while preserving production defaults when available.
#' @param cfg A named list from get_analysis_config().
#' @param import_folder Character scalar folder path.
#' @return Named list config for import read helpers.
#' @keywords internal
#' @noRd
.build_excel_read_benchmark_config <- function(cfg, import_folder) {
  pipeline_cfg <- NULL

  if (exists("load_pipeline_config", mode = "function")) {
    pipeline_cfg <- tryCatch(
      load_pipeline_config(),
      error = function(e) NULL
    )
  }

  default_cfg <- make_benchmark_config()
  read_cfg <- if (is.list(pipeline_cfg)) pipeline_cfg else default_cfg

  if (
    is.null(read_cfg$column_required) ||
      !is.character(read_cfg$column_required) ||
      length(read_cfg$column_required) == 0L
  ) {
    read_cfg$column_required <- default_cfg$column_required
  }

  if (is.null(read_cfg$paths)) {
    read_cfg$paths <- list()
  }
  if (is.null(read_cfg$paths$data)) {
    read_cfg$paths$data <- list()
  }
  if (is.null(read_cfg$paths$data$imports)) {
    read_cfg$paths$data$imports <- list()
  }
  read_cfg$paths$data$imports$raw <- import_folder

  return(read_cfg)
}

#' @title Select Excel files for workload
#' @description Deterministically cycle over discovered file paths to produce a
#'   fixed-length read list for one benchmark invocation.
#' @param file_paths Character vector of discovered xlsx paths.
#' @param n_reads Integer scalar number of reads to execute.
#' @return Character vector of file paths to read.
#' @keywords internal
#' @noRd
.select_excel_files_for_workload <- function(file_paths, n_reads) {
  if (length(file_paths) == 0L || n_reads <= 0L) {
    return(character(0))
  }
  idx <- ((seq_len(n_reads) - 1L) %% length(file_paths)) + 1L
  return(as.character(file_paths[idx]))
}

# ── 5a. stage 0 — general pipeline ──────────────────────────────────────────

#' @title Build stage 0 benchmarks
#' @description Internal builder for stage 0 benchmark descriptors.
#' @param cfg A named list from get_analysis_config().
#' @return A list of benchmark descriptors.
#' @keywords internal
#' @noRd
.build_stage_0_general_benchmarks <- function(cfg) {
  na_frac <- cfg$na_fraction
  list(
    list(
      name = "normalize_string_impl",
      stage = "0-general",
      description = "normalise a character vector of length n to ASCII lowercase",
      fn_factory = function(n) {
        x <- paste0(
          sample(LETTERS, n, replace = TRUE),
          sample(0:9, n, replace = TRUE),
          sample(
            c("\u00e9", "\u00f1", "\u00fc", " ", "-", "_"),
            n,
            replace = TRUE
          )
        )
        function() normalize_string_impl(x)
      }
    ),

    list(
      name = "coerce_numeric_safe",
      stage = "0-general",
      description = "coerce a character vector of length n to numeric",
      fn_factory = function(n) {
        x <- make_numeric_string_vec(n)
        function() coerce_numeric_safe(x)
      }
    ),

    list(
      name = "drop_na_value_rows",
      stage = "0-general",
      description = paste0(
        "filter NA-value rows from a data.table of n rows (",
        round(na_frac * 100L),
        "% NA)"
      ),
      fn_factory = function(n) {
        dt <- make_long_dt(n, na_fraction = na_frac)
        function() drop_na_value_rows(dt)
      }
    )
  )
}

# ── 5b. stage 1 — import pipeline ────────────────────────────────────────────

#' @title Build stage 1 benchmarks
#' @description Internal builder for stage 1 benchmark descriptors.
#' @param cfg A named list from get_analysis_config().
#' @return A list of benchmark descriptors.
#' @keywords internal
#' @noRd
.build_stage_1_import_benchmarks <- function(cfg) {
  n_yrs <- cfg$n_year_cols
  dup_frac <- cfg$dup_fraction
  bench_cfg <- make_benchmark_config()
  excel_fixture_copy_count <- max(
    1L,
    .resolve_integer_cfg(cfg$excel_read_fixture_copy_count, fallback = 4L)
  )
  excel_max_reads <- max(
    1L,
    .resolve_integer_cfg(cfg$excel_read_max_reads_per_iteration, fallback = 8L)
  )

  list(
    list(
      name = "normalize_key_fields",
      stage = "1-import",
      description = "normalize product/variable/continent/country in n-row wide table",
      fn_factory = function(n) {
        df <- make_wide_dt(n, n_years = n_yrs)
        function() {
          normalize_key_fields(data.table::copy(df), "cereals", bench_cfg)
        }
      }
    ),

    list(
      name = "reshape_to_long",
      stage = "1-import",
      description = paste0(
        "melt n-row wide table (",
        n_yrs,
        " year cols) to long format"
      ),
      fn_factory = function(n) {
        df <- make_wide_dt(n, n_years = n_yrs)
        attr(df, "whep_year_columns") <- identify_year_columns(df, bench_cfg)
        function() reshape_to_long(df, bench_cfg)
      }
    ),

    list(
      name = "validate_mandatory_fields_dt",
      stage = "1-import",
      description = "check mandatory non-empty fields in n-row long table",
      fn_factory = function(n) {
        dt <- make_long_dt(n, na_fraction = 0.05)
        function() validate_mandatory_fields_dt(dt, bench_cfg)
      }
    ),

    list(
      name = "detect_duplicates_dt",
      stage = "1-import",
      description = paste0(
        "detect duplicate keys in n-row long table (",
        round(dup_frac * 100L),
        "% dups)"
      ),
      fn_factory = function(n) {
        dt <- make_long_dt(n, dup_fraction = dup_frac)
        function() detect_duplicates_dt(dt)
      }
    ),

    list(
      name = "consolidate_audited_dt",
      stage = "1-import",
      description = "consolidate and reorder columns in a list of n-row long tables",
      fn_factory = function(n) {
        chunk <- max(1L, n %/% 3L)
        dt_list <- list(
          make_long_dt(chunk),
          make_long_dt(chunk),
          make_long_dt(n - 2L * chunk)
        )
        local_cfg <- bench_cfg
        function() {
          consolidate_audited_dt(dt_list, local_cfg)
        }
      }
    ),

    list(
      name = "discover_and_read_excel_sheets",
      stage = "1-import",
      description = "discover import xlsx files and read sheets via pipeline readers",
      fn_factory = function(n) {
        import_folder <- .resolve_excel_benchmark_import_folder(
          cfg = cfg,
          fixture_copy_count = excel_fixture_copy_count
        )
        read_cfg <- .build_excel_read_benchmark_config(cfg, import_folder)
        reads_per_call <- .scale_excel_read_iterations(
          n = n,
          max_reads = excel_max_reads
        )

        function() {
          file_meta <- discover_pipeline_files(read_cfg)
          available_paths <- as.character(file_meta$file_path)
          available_paths <- available_paths[file.exists(available_paths)]

          if (length(available_paths) == 0L) {
            return(invisible(list(rows = 0L, errors = 0L)))
          }

          selected_paths <- .select_excel_files_for_workload(
            file_paths = available_paths,
            n_reads = reads_per_call
          )

          read_results <- lapply(selected_paths, function(path_i) {
            read_file_sheets(path_i, read_cfg)
          })

          total_rows <- sum(vapply(read_results, function(result_i) {
            if (is.null(result_i$data)) {
              return(0L)
            }
            as.integer(nrow(result_i$data))
          }, integer(1)))

          total_errors <- sum(vapply(read_results, function(result_i) {
            if (is.null(result_i$errors)) {
              return(0L)
            }
            as.integer(length(result_i$errors))
          }, integer(1)))

          return(invisible(list(rows = total_rows, errors = total_errors)))
        }
      }
    )
  )
}

# ── 5c. stage 2 — post-processing pipeline ───────────────────────────────────

#' @title Build stage 2 benchmarks
#' @description Internal builder for stage 2 benchmark descriptors.
#' @param cfg A named list from get_analysis_config().
#' @return A list of benchmark descriptors.
#' @keywords internal
#' @noRd
.build_stage_2_post_processing_benchmarks <- function(cfg) {
  dup_frac <- cfg$dup_fraction
  na_frac <- cfg$na_fraction

  conversion_rules_raw <- data.table::data.table(
    product = rep(.ca_products, each = 3L),
    source_unit = rep(c("tonnes", "kg_ha", "ha"), times = length(.ca_products)),
    target_unit = rep(c("kg", "kg_per_ha_std", "ha_std"), times = length(.ca_products)),
    multiplier = rep(c(1000, 1, 1), times = length(.ca_products)),
    addend = 0
  )

  list(
    list(
      name = "apply_standardize_rules",
      stage = "2-post_processing",
      description = paste0(
        "apply prepared unit conversion rules to an n-row long table (",
        round(na_frac * 100L),
        "% NA values, ",
        round(dup_frac * 100L),
        "% duplicates)"
      ),
      fn_factory = function(n) {
        dt_raw <- make_long_dt(n, na_fraction = na_frac, dup_fraction = dup_frac)
        prepared_rules <- prepare_standardize_rules(conversion_rules_raw)
        function() {
          apply_standardize_rules(
            mapped_dt = dt_raw,
            prepared_rules_dt = prepared_rules,
            unit_column = "unit",
            value_column = "value",
            product_column = "product"
          )
        }
      }
    ),

    list(
      name = "extract_aggregated_rows",
      stage = "2-post_processing",
      description = paste0(
        "extract duplicate groups from n-row standardized table (",
        round(dup_frac * 100L),
        "% duplicates)"
      ),
      fn_factory = function(n) {
        dt_raw <- make_long_dt(n, dup_fraction = dup_frac)
        dt_raw[, value := suppressWarnings(as.numeric(value))]
        function() extract_aggregated_rows(data.table::copy(dt_raw))
      }
    ),

    list(
      name = "aggregate_standardized_rows",
      stage = "2-post_processing",
      description = paste0(
        "group-sum n rows by all non-value columns (",
        round(dup_frac * 100L),
        "% duplicates to aggregate)"
      ),
      fn_factory = function(n) {
        dt_raw <- make_long_dt(n, dup_fraction = dup_frac)
        dt_raw[, value := suppressWarnings(as.numeric(value))]
        function() aggregate_standardized_rows(data.table::copy(dt_raw))
      }
    )
  )
}

# ── 5d. stage 3 — export pipeline ────────────────────────────────────────────

#' @title Build stage 3 benchmarks
#' @description Internal builder for stage 3 benchmark descriptors.
#' @param cfg A named list from get_analysis_config().
#' @return A list of benchmark descriptors.
#' @keywords internal
#' @noRd
.build_stage_3_export_benchmarks <- function(cfg) {
  list(
    list(
      name = "compute_unique_column_values",
      stage = "3-export",
      description = "compute sorted unique values for one column of n-row table",
      fn_factory = function(n) {
        dt <- make_long_dt(n)
        function() compute_unique_column_values(dt, "country")
      }
    ),

    list(
      name = "normalize_for_comparison",
      stage = "3-export",
      description = "deep-copy, drop year col, sort columns and rows of n-row table",
      fn_factory = function(n) {
        dt <- make_long_dt(n)
        function() normalize_for_comparison(data.table::copy(dt))
      }
    )
  )
}

# ── 5e. public builders ───────────────────────────────────────────────────────

#' @title Build stage benchmarks
#' @description Return benchmark descriptors for one pipeline stage.
#' @param stage_id A character scalar stage identifier.
#' @param cfg A named list from get_analysis_config().
#' @return A named list of benchmark descriptors.
build_stage_benchmarks <- function(stage_id, cfg) {
  benchmarks <- switch(
    stage_id,
    "0-general" = .build_stage_0_general_benchmarks(cfg),
    "1-import" = .build_stage_1_import_benchmarks(cfg),
    "2-post_processing" = .build_stage_2_post_processing_benchmarks(cfg),
    "3-export" = .build_stage_3_export_benchmarks(cfg),
    stop(sprintf("unknown stage_id: '%s'", stage_id))
  )
  names(benchmarks) <- vapply(benchmarks, function(b) b$name, character(1))
  return(benchmarks)
}

#' @title Build benchmark definitions
#' @description Return the full benchmark descriptor catalog across configured
#'   stages.
#' @param cfg A named list from get_analysis_config().
#' @return A named list of benchmark descriptors.
build_benchmark_definitions <- function(cfg) {
  set.seed(cfg$rng_seed)
  all_benchmarks <- lapply(cfg$stages, build_stage_benchmarks, cfg = cfg)
  combined <- do.call(c, all_benchmarks)
  names(combined) <- vapply(combined, function(b) b$name, character(1))
  return(combined)
}

#' @title Build all benchmarks
#' @description Alias for build_benchmark_definitions().
#' @param cfg A named list from get_analysis_config().
#' @return A named list of benchmark descriptors.
build_all_benchmarks <- function(cfg) build_benchmark_definitions(cfg)
