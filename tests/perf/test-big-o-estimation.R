# tests/perf/test-big-o-estimation.R
# unit tests for the perf module.
#
# the module is split into modular scripts (p0-dependencies.R ... p9-orchestration.R)
# sourced by perf/p9-orchestration.R. this test file
# sources the master script, which loads all sub-modules and exposes the
# full public API.
#
# covers: config helpers, synthetic data generators, complexity model fitting,
# benchmark summary statistics, and Markdown serialisation.

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here("perf", "p9-orchestration.R"),
  echo = FALSE
)


# ── get_analysis_config ──────────────────────────────────────────────────────────

testthat::test_that("get_analysis_config returns a list with all required fields", {
  cfg <- get_analysis_config()

  testthat::expect_type(cfg, "list")
  testthat::expect_true("input_sizes"   %in% names(cfg))
  testthat::expect_true("n_reps"        %in% names(cfg))
  testthat::expect_true("n_year_cols"   %in% names(cfg))
  testthat::expect_true("na_fraction"   %in% names(cfg))
  testthat::expect_true("dup_fraction"  %in% names(cfg))
  testthat::expect_true("rng_seed"      %in% names(cfg))
  testthat::expect_true("produce_plots" %in% names(cfg))
  testthat::expect_true("quiet"         %in% names(cfg))
})

testthat::test_that("get_analysis_config input_sizes are positive integers", {
  cfg <- get_analysis_config()
  testthat::expect_true(all(cfg$input_sizes > 0L))
  testthat::expect_true(is.integer(cfg$input_sizes))
})

testthat::test_that("get_analysis_config fractions are in [0, 1]", {
  cfg <- get_analysis_config()
  testthat::expect_true(cfg$na_fraction  >= 0 & cfg$na_fraction  <= 1)
  testthat::expect_true(cfg$dup_fraction >= 0 & cfg$dup_fraction <= 1)
})


# ── make_benchmark_config ─────────────────────────────────────────────────────

testthat::test_that("make_benchmark_config contains required column specs", {
  cfg <- make_benchmark_config()

  testthat::expect_type(cfg, "list")
  testthat::expect_true("column_required" %in% names(cfg))
  testthat::expect_true("column_id"       %in% names(cfg))
  testthat::expect_true("column_order"    %in% names(cfg))
  testthat::expect_true("defaults"        %in% names(cfg))
})

testthat::test_that("make_benchmark_config column_order includes full target schema", {
  cfg <- make_benchmark_config()
  # consolidate_audited_dt checks that all of these are in column_order
  required_schema <- c(
    "hemisphere", "continent", "country", "product", "variable",
    "unit", "year", "value", "notes", "footnotes", "yearbook", "document"
  )
  testthat::expect_true(
    all(required_schema %in% cfg$column_order),
    info = paste("missing:", paste(setdiff(required_schema, cfg$column_order),
                                   collapse = ", "))
  )
})

testthat::test_that("make_benchmark_config column_order has no duplicates", {
  cfg <- make_benchmark_config()
  testthat::expect_equal(length(cfg$column_order), length(unique(cfg$column_order)))
})


# ── make_wide_dt ──────────────────────────────────────────────────────────────

testthat::test_that("make_wide_dt returns data.table with correct row count", {
  dt <- make_wide_dt(200L)
  testthat::expect_true(data.table::is.data.table(dt))
  testthat::expect_equal(nrow(dt), 200L)
})

testthat::test_that("make_wide_dt returns correct number of year columns", {
  dt <- make_wide_dt(50L, n_years = 5L)
  year_cols <- grep("^\\d{4}$", names(dt), value = TRUE)
  testthat::expect_equal(length(year_cols), 5L)
})

testthat::test_that("make_wide_dt year column names are sequential years", {
  dt <- make_wide_dt(10L, n_years = 3L)
  year_cols <- grep("^\\d{4}$", names(dt), value = TRUE)
  testthat::expect_equal(sort(year_cols), c("2000", "2001", "2002"))
})

testthat::test_that("make_wide_dt contains required base columns", {
  dt    <- make_wide_dt(20L)
  required <- c("product", "variable", "unit", "continent", "country", "footnotes")
  testthat::expect_true(all(required %in% names(dt)))
})


# ── make_long_dt ──────────────────────────────────────────────────────────────

testthat::test_that("make_long_dt returns data.table with correct row count", {
  dt <- make_long_dt(300L)
  testthat::expect_true(data.table::is.data.table(dt))
  testthat::expect_equal(nrow(dt), 300L)
})

testthat::test_that("make_long_dt contains full long-format schema columns", {
  dt       <- make_long_dt(10L)
  required <- c("product", "variable", "unit", "continent", "country",
                "year", "value", "notes", "yearbook", "document", "footnotes")
  testthat::expect_true(all(required %in% names(dt)))
})

testthat::test_that("make_long_dt respects na_fraction", {
  set.seed(1L)
  dt       <- make_long_dt(2000L, na_fraction = 0.5)
  na_share <- mean(is.na(dt$value))
  # allow generous tolerance due to random sampling
  testthat::expect_true(na_share > 0.3 & na_share < 0.7)
})

testthat::test_that("make_long_dt respects dup_fraction", {
  set.seed(2L)
  n   <- 1000L
  dt  <- make_long_dt(n, dup_fraction = 0.2)
  # total rows should equal n
  testthat::expect_equal(nrow(dt), n)
})


# ── make_numeric_string_vec ───────────────────────────────────────────────────

testthat::test_that("make_numeric_string_vec returns character vector of length n", {
  vec <- make_numeric_string_vec(150L)
  testthat::expect_type(vec, "character")
  testthat::expect_equal(length(vec), 150L)
})

testthat::test_that("make_numeric_string_vec contains mix of valid numerics, empty, NA", {
  set.seed(7L)
  vec      <- make_numeric_string_vec(500L)
  has_na   <- any(is.na(vec))
  has_empty <- any(vec == "", na.rm = TRUE)
  has_num  <- any(!is.na(suppressWarnings(as.numeric(vec[!is.na(vec)]))))
  testthat::expect_true(has_na)
  testthat::expect_true(has_empty)
  testthat::expect_true(has_num)
})


# ── fit_complexity_model ──────────────────────────────────────────────────────

testthat::test_that("fit_complexity_model identifies O(1) from flat data", {
  set.seed(10L)
  n   <- c(100, 500, 1000, 2500, 5000, 10000, 25000, 50000)
  t   <- rep(0.01, length(n)) + stats::rnorm(length(n), 0, 0.0001)
  fit <- fit_complexity_model(n, t)

  testthat::expect_type(fit, "list")
  testthat::expect_true("best_class" %in% names(fit))
  testthat::expect_equal(fit$best_class, "O(1)")
})

testthat::test_that("fit_complexity_model identifies O(n) from linear data", {
  n   <- c(100, 500, 1000, 2500, 5000, 10000, 25000, 50000)
  t   <- 2e-6 * n + 0.001
  fit <- fit_complexity_model(n, t)

  testthat::expect_equal(fit$best_class, "O(n)")
  testthat::expect_true(!is.na(fit$best_r2))
  testthat::expect_true(fit$best_r2 > 0.99)
})

testthat::test_that("fit_complexity_model identifies O(n^2) from quadratic data", {
  n   <- c(100, 500, 1000, 2500, 5000, 10000)
  t   <- 1e-10 * n^2 + 0.0005
  fit <- fit_complexity_model(n, t)

  testthat::expect_equal(fit$best_class, "O(n^2)")
  testthat::expect_true(fit$best_r2 > 0.99)
})

testthat::test_that("fit_complexity_model returns 'unknown' with too few points", {
  fit <- fit_complexity_model(c(100, 200), c(0.01, 0.02))
  testthat::expect_equal(fit$best_class, "unknown")
  testthat::expect_true(is.na(fit$best_r2))
})

testthat::test_that("fit_complexity_model returns all_r2 named vector", {
  n   <- c(100, 500, 1000, 2500, 5000, 10000)
  t   <- 1e-6 * n + 0.001
  fit <- fit_complexity_model(n, t)

  testthat::expect_type(fit$all_r2, "double")
  testthat::expect_equal(
    sort(names(fit$all_r2)),
    sort(c("O(1)", "O(log n)", "O(n)", "O(n log n)", "O(n^2)", "O(n^3)"))
  )
})

testthat::test_that("fit_complexity_model handles NA and negative times gracefully", {
  n   <- c(100, 500, 1000, NA, 5000, 10000)
  t   <- c(0.001, 0.005, 0.010, NA, 0.050, 0.100)
  fit <- fit_complexity_model(n, t)

  testthat::expect_type(fit$best_class, "character")
  testthat::expect_length(fit$best_class, 1L)
})


# ── summarise_benchmark ───────────────────────────────────────────────────────

testthat::test_that("summarise_benchmark returns correct per-n statistics", {
  raw_dt <- data.table::data.table(
    n         = c(100L, 100L, 100L, 500L, 500L, 500L),
    rep       = c(1L, 2L, 3L, 1L, 2L, 3L),
    elapsed_s = c(0.01, 0.02, 0.03, 0.05, 0.06, 0.07)
  )
  summ <- summarise_benchmark(raw_dt)

  testthat::expect_true(data.table::is.data.table(summ))
  testthat::expect_equal(nrow(summ), 2L)
  testthat::expect_true(all(c("median_s", "mean_s", "sd_s", "min_s", "max_s") %in%
                              names(summ)))
})

testthat::test_that("summarise_benchmark values are correct", {
  raw_dt <- data.table::data.table(
    n         = c(100L, 100L, 100L),
    rep       = 1L:3L,
    elapsed_s = c(0.01, 0.02, 0.03)
  )
  summ <- summarise_benchmark(raw_dt)

  testthat::expect_equal(summ$median_s, 0.02)
  testthat::expect_equal(summ$min_s,    0.01)
  testthat::expect_equal(summ$max_s,    0.03)
})

testthat::test_that("summarise_benchmark output is sorted by n", {
  raw_dt <- data.table::data.table(
    n         = c(500L, 500L, 100L, 100L),
    rep       = c(1L, 2L, 1L, 2L),
    elapsed_s = c(0.05, 0.06, 0.01, 0.02)
  )
  summ <- summarise_benchmark(raw_dt)
  testthat::expect_equal(summ$n, c(100L, 500L))
})


# ── build_benchmark_definitions ───────────────────────────────────────────────

testthat::test_that("build_benchmark_definitions returns a named list", {
  cfg  <- utils::modifyList(get_analysis_config(), list(
    input_sizes = c(100L, 200L),
    n_reps      = 1L
  ))
  defs <- build_benchmark_definitions(cfg)

  testthat::expect_type(defs, "list")
  testthat::expect_true(length(defs) > 0L)
  testthat::expect_false(is.null(names(defs)))
})

testthat::test_that("each benchmark definition has required fields", {
  cfg  <- utils::modifyList(get_analysis_config(), list(
    input_sizes = c(100L),
    n_reps      = 1L
  ))
  defs <- build_benchmark_definitions(cfg)

  for (nm in names(defs)) {
    bm <- defs[[nm]]
    testthat::expect_true("name"        %in% names(bm), info = nm)
    testthat::expect_true("stage"       %in% names(bm), info = nm)
    testthat::expect_true("description" %in% names(bm), info = nm)
    testthat::expect_true("fn_factory"  %in% names(bm), info = nm)
    testthat::expect_type(bm$fn_factory, "closure")
  }
})

testthat::test_that("fn_factory returns a zero-argument function", {
  cfg  <- utils::modifyList(get_analysis_config(), list(
    input_sizes = c(100L),
    n_reps      = 1L
  ))
  defs <- build_benchmark_definitions(cfg)

  # test just the first benchmark to keep test fast
  bm <- defs[[1L]]
  fn <- bm$fn_factory(100L)
  testthat::expect_type(fn, "closure")
  testthat::expect_equal(length(formals(fn)), 0L)
})

testthat::test_that("stage 2 benchmark catalog includes additional post-processing hotspots", {
  cfg <- utils::modifyList(get_analysis_config(), list(
    stages = c("2-post_processing"),
    input_sizes = c(100L),
    n_reps = 1L
  ))

  defs <- build_benchmark_definitions(cfg)
  expected <- c(
    "apply_standardize_rules",
    "extract_aggregated_rows",
    "aggregate_standardized_rows"
  )

  testthat::expect_true(all(expected %in% names(defs)))
  testthat::expect_true(all(vapply(
    defs[expected],
    function(bm) identical(bm$stage, "2-post_processing"),
    logical(1)
  )))
})

testthat::test_that("stage 2 benchmark factories execute without error", {
  cfg <- utils::modifyList(get_analysis_config(), list(
    stages = c("2-post_processing"),
    input_sizes = c(100L),
    n_reps = 1L
  ))

  defs <- build_stage_benchmarks("2-post_processing", cfg)
  expected <- c(
    "apply_standardize_rules",
    "extract_aggregated_rows",
    "aggregate_standardized_rows"
  )

  for (name_i in expected) {
    fn <- defs[[name_i]]$fn_factory(100L)
    testthat::expect_type(fn, "closure")
    testthat::expect_no_error(fn())
  }
})


# ── export_results_markdown / build_analysis_markdown ────────────────────────

# helper: build a minimal mock results object accepted by export_results_markdown
make_mock_results <- function(fn_name = "bench_fn",
                              stage       = "1-import",
                              description = "a benchmark") {
  complexity_dt <- data.table::data.table(
    fn_name           = fn_name,
    stage             = stage,
    description       = description,
    best_class        = "O(n)",
    r_squared         = 0.99,
    slope_per_n       = 1e-6,
    dominant_in_stage = TRUE,
    complexity_rank   = 3L,
    stage_max_rank    = 3L
  )
  list(
    raw        = data.table::data.table(),
    summary    = data.table::data.table(),
    complexity = complexity_dt
  )
}

make_mock_results_with_runtime <- function() {
  complexity_dt <- data.table::data.table(
    fn_name           = c("bench_hot", "bench_cool"),
    stage             = c("1-import", "1-import"),
    description       = c("hot path", "support path"),
    best_class        = c("O(n^2)", "O(n)"),
    r_squared         = c(0.97, 0.95),
    slope_per_n       = c(2e-6, 8e-7),
    dominant_in_stage = c(TRUE, FALSE),
    complexity_rank   = c(5L, 3L),
    stage_max_rank    = c(5L, 5L)
  )

  summary_dt <- data.table::data.table(
    fn_name   = rep(c("bench_hot", "bench_cool"), each = 3L),
    stage     = rep("1-import", 6L),
    n         = rep(c(100L, 1000L, 5000L), 2L),
    median_s  = c(0.5, 1.0, 1.5, 0.2, 0.3, 0.5),
    mean_s    = c(0.5, 1.0, 1.5, 0.2, 0.3, 0.5),
    sd_s      = 0,
    min_s     = c(0.5, 1.0, 1.5, 0.2, 0.3, 0.5),
    max_s     = c(0.5, 1.0, 1.5, 0.2, 0.3, 0.5),
    n_reps    = 1L
  )

  list(
    raw        = data.table::data.table(),
    summary    = summary_dt,
    complexity = complexity_dt
  )
}

testthat::test_that("export_results_markdown writes a file without error", {
  results    <- make_mock_results()
  out_dir    <- tempfile("perf_reports_")
  on.exit(unlink(out_dir, recursive = TRUE), add = TRUE)

  testthat::expect_no_error(
    export_results_markdown(results, out_dir)
  )

  testthat::expect_true(file.exists(file.path(out_dir, "perf_whep-digitalization.md")))
  testthat::expect_true(file.exists(file.path(out_dir, "perf_0-general_pipeline.md")))
  testthat::expect_true(file.exists(file.path(out_dir, "perf_1-import_pipeline.md")))
  testthat::expect_true(file.exists(file.path(out_dir, "perf_2-post_processing_pipeline.md")))
  testthat::expect_true(file.exists(file.path(out_dir, "perf_3-export_pipeline.md")))
})

testthat::test_that("export_results_markdown output contains expected sections", {
  results  <- make_mock_results()
  out_dir  <- tempfile("perf_reports_")
  on.exit(unlink(out_dir, recursive = TRUE), add = TRUE)

  export_results_markdown(results, out_dir)
  pipeline_path <- file.path(out_dir, "perf_1-import_pipeline.md")
  general_path <- file.path(out_dir, "perf_whep-digitalization.md")

  raw_lines <- readLines(pipeline_path, warn = FALSE)
  raw_text <- paste(raw_lines, collapse = "\n")
  general_lines <- readLines(general_path, warn = FALSE)

  testthat::expect_true(any(grepl("^# Pipeline Performance Report: perf_1-import_pipeline$", raw_lines)))
  testthat::expect_true(any(grepl("^## Function-Level Performance Matrix$", raw_lines)))
  testthat::expect_true(any(grepl("^## Runtime Share Distribution \\(ASCII\\)$", raw_lines)))
  testthat::expect_true(any(grepl("^```text$", raw_lines)))
  testthat::expect_true(grepl("\\| Function\\s+\\| Description\\s+\\| Complexity\\s+\\| adj\\.R2\\s+\\| Slope per n\\s+\\| Estimated runtime \\(sample n\\)\\s+\\| Relative impact\\s+\\| Indicator\\s+\\| Bottleneck\\s+\\|", raw_text))
  testthat::expect_false(grepl("Impact bar", raw_text, fixed = TRUE))
  testthat::expect_true(any(grepl("^# General Project Performance$", general_lines)))
  testthat::expect_true(any(grepl("^## Pipeline Summary$", general_lines)))
})

testthat::test_that("export_results_markdown preserves strings with tab/newline control chars", {
  results  <- make_mock_results(description = "line1\nline2\ttabbed")
  out_dir <- tempfile("perf_reports_")
  on.exit(unlink(out_dir, recursive = TRUE), add = TRUE)

  testthat::expect_no_error(export_results_markdown(results, out_dir))

  raw_text <- paste(readLines(file.path(out_dir, "perf_1-import_pipeline.md"), warn = FALSE), collapse = "\n")
  testthat::expect_true(grepl("line1", raw_text, fixed = TRUE))
  testthat::expect_true(grepl("line2", raw_text, fixed = TRUE))
  testthat::expect_true(grepl("tabbed", raw_text, fixed = TRUE))
})

testthat::test_that("build_analysis_markdown handles NA r_squared", {
  results    <- make_mock_results()
  results$complexity[, r_squared := NA_real_]
  lines <- build_analysis_markdown(results)
  testthat::expect_true(any(grepl("N/A", lines, fixed = TRUE)))
})

testthat::test_that("prepare_reporting_metrics computes stage and function impact metrics", {
  results <- make_mock_results_with_runtime()
  metrics <- prepare_reporting_metrics(results)

  testthat::expect_true(data.table::is.data.table(metrics$stage_summary))
  testthat::expect_equal(metrics$stage_summary$function_count[[1L]], 2L)
  testthat::expect_equal(metrics$stage_summary$expensive_function_count[[1L]], 1L)
  testthat::expect_equal(round(metrics$stage_summary$expensive_function_pct[[1L]], 1), 50.0)

  hot_impact <- metrics$function_metrics[fn_name == "bench_hot", relative_impact][[1L]]
  testthat::expect_equal(round(hot_impact, 2), 0.75)
  testthat::expect_true(all(c(100L, 1000L, 5000L) %in% metrics$sample_n_values))
})

testthat::test_that("build_analysis_markdown includes chart-ready and projection sections", {
  results <- make_mock_results_with_runtime()
  lines <- build_analysis_markdown(results)
  raw_text <- paste(lines, collapse = "\n")

  testthat::expect_true(any(grepl("^## Chart Data: Function Complexities$", lines)))
  testthat::expect_true(any(grepl("^## Chart Data: Stage Runtime Proportions$", lines)))
  testthat::expect_true(any(grepl("^## Chart Data: Slope per n and Relative Impact$", lines)))
  testthat::expect_true(any(grepl("^## Runtime Projections by Sample n$", lines)))
  testthat::expect_true(grepl("75.0%", raw_text, fixed = TRUE))
  testthat::expect_true(grepl("!!! (critical)", raw_text, fixed = TRUE))
})

# ── run_benchmark (progressor integration) ───────────────────────────────────

testthat::test_that("run_benchmark accepts a progressor and calls it once per input size", {
  sizes   <- c(100L, 200L, 300L)
  n_calls <- 0L

  # a mock progressor that simply counts calls
  mock_progressor <- function(msg = NULL) {
    n_calls <<- n_calls + 1L
  }

  fn_factory <- function(n) function() Sys.sleep(0)

  result <- run_benchmark(fn_factory, sizes, n_reps = 1L,
                          quiet = TRUE, progressor = mock_progressor)

  testthat::expect_equal(n_calls, length(sizes))
})

testthat::test_that("run_benchmark progressor message contains n and fraction", {
  sizes    <- c(100L, 500L)
  messages <- character(0L)

  mock_progressor <- function(msg = NULL) {
    if (!is.null(msg)) messages <<- c(messages, msg)
  }

  fn_factory <- function(n) function() Sys.sleep(0)

  run_benchmark(fn_factory, sizes, n_reps = 1L,
                quiet = TRUE, progressor = mock_progressor)

  # each message should mention the size and the i/T fraction
  testthat::expect_true(any(grepl("100", messages)))
  testthat::expect_true(any(grepl("500", messages)))
  testthat::expect_true(any(grepl("1/2", messages)))
  testthat::expect_true(any(grepl("2/2", messages)))
})

testthat::test_that("run_benchmark with quiet=TRUE and no progressor emits no messages", {
  fn_factory <- function(n) function() Sys.sleep(0)
  sizes      <- c(50L, 100L)

  msgs <- character(0L)
  withCallingHandlers(
    run_benchmark(fn_factory, sizes, n_reps = 1L, quiet = TRUE, progressor = NULL),
    message = function(m) {
      msgs <<- c(msgs, conditionMessage(m))
      invokeRestart("muffleMessage")
    }
  )

  testthat::expect_length(msgs, 0L)
})

testthat::test_that("run_benchmark returns correct data.table structure when progressor is used", {
  sizes <- c(100L, 200L)
  mock_progressor <- function(msg = NULL) invisible(NULL)
  fn_factory <- function(n) function() Sys.sleep(0)

  result <- run_benchmark(fn_factory, sizes, n_reps = 2L,
                          quiet = TRUE, progressor = mock_progressor)

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true(all(c("n", "rep", "elapsed_s") %in% names(result)))
  testthat::expect_equal(nrow(result), length(sizes) * 2L)
})
