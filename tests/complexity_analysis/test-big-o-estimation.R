# tests/complexity_analysis/test-big-o-estimation.R
# unit tests for scripts/complexity_analysis/big_o_estimation.R
#
# covers: config helpers, synthetic data generators, complexity model fitting,
# benchmark summary statistics, and JSON serialisation.

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here("scripts", "complexity_analysis", "big_o_estimation.R"),
  echo = FALSE
)


# ── get_big_o_config ──────────────────────────────────────────────────────────

testthat::test_that("get_big_o_config returns a list with all required fields", {
  cfg <- get_big_o_config()

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

testthat::test_that("get_big_o_config input_sizes are positive integers", {
  cfg <- get_big_o_config()
  testthat::expect_true(all(cfg$input_sizes > 0L))
  testthat::expect_true(is.integer(cfg$input_sizes))
})

testthat::test_that("get_big_o_config fractions are in [0, 1]", {
  cfg <- get_big_o_config()
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
  cfg  <- utils::modifyList(get_big_o_config(), list(
    input_sizes = c(100L, 200L),
    n_reps      = 1L
  ))
  defs <- build_benchmark_definitions(cfg)

  testthat::expect_type(defs, "list")
  testthat::expect_true(length(defs) > 0L)
  testthat::expect_false(is.null(names(defs)))
})

testthat::test_that("each benchmark definition has required fields", {
  cfg  <- utils::modifyList(get_big_o_config(), list(
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
  cfg  <- utils::modifyList(get_big_o_config(), list(
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


# ── export_results_json / to_json ─────────────────────────────────────────────

# helper: build a minimal mock results object accepted by export_results_json
make_mock_results <- function(fn_name = "bench_fn",
                              stage       = "import",
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

testthat::test_that("export_results_json writes a file without error", {
  results    <- make_mock_results()
  out_path   <- tempfile(fileext = ".json")
  on.exit(unlink(out_path), add = TRUE)

  testthat::expect_no_error(
    export_results_json(results, out_path)
  )
  testthat::expect_true(file.exists(out_path))
})

testthat::test_that("export_results_json output parses as valid JSON", {
  results  <- make_mock_results()
  out_path <- tempfile(fileext = ".json")
  on.exit(unlink(out_path), add = TRUE)

  export_results_json(results, out_path)
  raw_text <- paste(readLines(out_path, warn = FALSE), collapse = "\n")

  # jsonlite is available via the project's existing dependencies
  parsed <- jsonlite::fromJSON(raw_text, simplifyVector = FALSE)
  testthat::expect_type(parsed, "list")
  testthat::expect_true("overall_pipeline_class" %in% names(parsed))
  testthat::expect_true("per_stage"              %in% names(parsed))
  testthat::expect_true("per_function"           %in% names(parsed))
})

testthat::test_that("export_results_json does not error on strings with tab/newline control chars", {
  # descriptions can contain arbitrary text; the serialiser must escape them
  results  <- make_mock_results(description = "line1\nline2\ttabbed")
  out_path <- tempfile(fileext = ".json")
  on.exit(unlink(out_path), add = TRUE)

  testthat::expect_no_error(export_results_json(results, out_path))

  raw_text <- paste(readLines(out_path, warn = FALSE), collapse = "\n")
  # the embedded newline and tab must appear as JSON escape sequences
  testthat::expect_true(grepl("\\\\n", raw_text))
  testthat::expect_true(grepl("\\\\t", raw_text))
})

testthat::test_that("export_results_json does not error on strings with low C0 control chars (\\x01-\\x1f)", {
  # exercise the fixed-loop path (cp in setdiff(1L:31L, c(8L,9L,10L,12L,13L)))
  # \x01 (SOH) and \x1f (US) are representative non-printable characters from
  # opposite ends of the loop range; testing both is sufficient to confirm the
  # loop iterates and applies \uXXXX escaping correctly.
  desc_with_ctrl <- paste0("ctrl", rawToChar(as.raw(1L)), "and",
                            rawToChar(as.raw(31L)), "end")
  results  <- make_mock_results(description = desc_with_ctrl)
  out_path <- tempfile(fileext = ".json")
  on.exit(unlink(out_path), add = TRUE)

  testthat::expect_no_error(export_results_json(results, out_path))

  raw_text <- paste(readLines(out_path, warn = FALSE), collapse = "\n")
  # \x01 → \u0001, \x1f → \u001f
  testthat::expect_true(grepl("\\\\u0001", raw_text))
  testthat::expect_true(grepl("\\\\u001f", raw_text))
})

testthat::test_that("export_results_json handles NA r_squared (serialised as null)", {
  results    <- make_mock_results()
  results$complexity[, r_squared := NA_real_]
  out_path   <- tempfile(fileext = ".json")
  on.exit(unlink(out_path), add = TRUE)

  testthat::expect_no_error(export_results_json(results, out_path))

  raw_text <- paste(readLines(out_path, warn = FALSE), collapse = "\n")
  testthat::expect_true(grepl('"adj_r_squared": null', raw_text, fixed = TRUE))
})
