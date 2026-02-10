# ============================================================
# Script:  run_export_pipeline.R
# Purpose: Self-contained orchestrator for exporting
#          final dataset and unique-column lists
#          with a clean, full-length progress bar.
# ============================================================

# ------------------------------
# 0. Source internal export scripts
# ------------------------------
export_scripts <- c(
  "31-export_data.R",
  "32-export_lists.R"
)

purrr::walk(
  export_scripts,
  ~ source(here::here("R/3-export_pipeline", .x), echo = FALSE)
)

# ------------------------------
# 1. Check that final_dt exists
# ------------------------------
if (!exists("final_dt")) {
  stop(
    "final_dt not found in the environment. Make sure the import pipeline has run."
  )
}

# ------------------------------
# 2. Run export pipeline with full-length progress bar
# ------------------------------
run_export_pipeline <- function(final_dt, config, overwrite = TRUE) {
  final_dt <- ensure_data_table(final_dt)
  cols_to_export <- config$export_config$lists_to_export
  total_steps <- 1 + length(cols_to_export) # 1 for final dataset + n for unique columns

  # Progress bar identical to reading pipeline, full length
  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    clear = FALSE
  ))

  progressr::with_progress({
    p <- progressr::progressor(along = seq_len(total_steps))

    # 1. Export final dataset
    export_processed_data(
      final_dt,
      config,
      base_name = "final",
      overwrite = overwrite
    )
    p() # increment progress

    # 2. Export unique-column lists
    purrr::walk(cols_to_export, function(col_name) {
      export_single_column_list(final_dt, col_name, config, overwrite)
      p() # increment progress
    })
  })

  invisible(TRUE)
}

# ------------------------------
# 3. Execute automatically
# ------------------------------
run_export_pipeline(final_dt, config, overwrite = TRUE)

# ------------------------------
# End of script
# ------------------------------