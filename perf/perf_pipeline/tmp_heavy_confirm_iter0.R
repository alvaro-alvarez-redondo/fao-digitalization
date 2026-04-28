suppressPackageStartupMessages({
  library(data.table)
  library(here)
})

source(
  here::here("perf", "perf_pipeline", "p9-orchestration.R"),
  echo = FALSE,
  local = FALSE
)

args <- commandArgs(trailingOnly = TRUE)
iter_label <- if (length(args) > 0L && nzchar(args[[1L]])) {
  args[[1L]]
} else {
  "iter0"
}

cfg <- get_analysis_config()
cfg$input_sizes <- as.integer(c(5000L, 10000L, 25000L, 50000L, 100000L))
cfg$n_reps <- 7L
cfg$n_year_cols <- 30L
cfg$na_fraction <- 0.05
cfg$dup_fraction <- 0.20
cfg$rng_seed <- 42L
cfg$excel_fixture_sheet_count <- 3L
cfg$excel_fixture_base_rows_per_sheet <- 96L
cfg$excel_discovery_scale_divisor <- 250L
cfg$excel_discovery_max_workbooks <- 96L
cfg$excel_discovery_repeats_per_iteration <- 80L
cfg$excel_read_workbook_count <- 6L
cfg$excel_read_rows_per_sheet <- 384L
cfg$excel_read_max_sheet_reads_per_iteration <- 45L
cfg$excel_read_repeats_per_iteration <- 4L

hotspots <- data.table::data.table(
  stage = c("1-import", "2-postpro", "2-postpro", "2-postpro"),
  fn_name = c(
    "read_excel_file_sheets",
    "apply_standardize_rules",
    "aggregate_standardized_rows",
    "extract_aggregated_rows"
  )
)

run_hotspot <- function(stage_id, fn_name, row_index) {
  cat(sprintf(
    "[hotspot %d/%d] %s :: %s\n",
    row_index,
    nrow(hotspots),
    stage_id,
    fn_name
  ))
  set.seed(cfg$rng_seed + as.integer(row_index))
  defs <- build_stage_benchmarks(stage_id, cfg)
  bm <- defs[[fn_name]]
  if (is.null(bm)) {
    stop(sprintf("benchmark not found: %s (%s)", fn_name, stage_id))
  }

  raw_dt <- run_benchmark(
    fn_factory = bm$fn_factory,
    input_sizes = cfg$input_sizes,
    n_reps = cfg$n_reps,
    quiet = TRUE,
    progressor = NULL
  )

  raw_dt[, `:=`(stage = stage_id, fn_name = fn_name)]

  summary_dt <- summarise_benchmark(raw_dt)
  summary_dt[, `:=`(stage = stage_id, fn_name = fn_name)]

  list(raw = raw_dt, summary = summary_dt)
}

cat(sprintf(
  "[start] heavy confirmation %s at %s\n",
  iter_label,
  format(Sys.time(), "%Y-%m-%d %H:%M:%S")
))

results <- vector("list", nrow(hotspots))
for (idx in seq_len(nrow(hotspots))) {
  results[[idx]] <- run_hotspot(
    stage_id = hotspots$stage[[idx]],
    fn_name = hotspots$fn_name[[idx]],
    row_index = idx
  )
}

raw_dt <- data.table::rbindlist(
  lapply(results, function(x) x$raw),
  use.names = TRUE,
  fill = TRUE
)
summary_dt <- data.table::rbindlist(
  lapply(results, function(x) x$summary),
  use.names = TRUE,
  fill = TRUE
)

leaderboard <- summary_dt[,
  .(
    overall_median_s = stats::median(median_s),
    max_n = max(as.integer(n)),
    max_n_median_s = median_s[which.max(n)],
    p95_at_max_n_s = p95_s[which.max(n)],
    mean_cv = mean(cv_s, na.rm = TRUE)
  ),
  by = .(stage, fn_name)
][order(-max_n_median_s, -overall_median_s)]

output_dir <- here::here(
  "perf",
  "perf_diagnosis",
  "perf_diagnosis_targeted",
  "iterative_loop"
)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

data.table::fwrite(
  raw_dt,
  file.path(output_dir, sprintf("heavy_confirm_%s_raw.csv", iter_label))
)
data.table::fwrite(
  summary_dt,
  file.path(output_dir, sprintf("heavy_confirm_%s_summary.csv", iter_label))
)
data.table::fwrite(
  leaderboard,
  file.path(output_dir, sprintf("heavy_confirm_%s_leaderboard.csv", iter_label))
)

cat(sprintf(
  "[done] heavy confirmation %s at %s\n",
  iter_label,
  format(Sys.time(), "%Y-%m-%d %H:%M:%S")
))
print(leaderboard)
