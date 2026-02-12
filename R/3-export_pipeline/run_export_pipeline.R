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
# 1. Check that fao_data_raw exists
# ------------------------------
if (!exists("fao_data_raw")) {
  stop(
    "fao_data_raw not found in the environment. Make sure the import pipeline has run."
  )
}

# ------------------------------
# 2. Run export pipeline with full-length progress bar
# ------------------------------
run_export_pipeline <- function(fao_data_raw, config, overwrite = TRUE) {
  fao_data_raw <- ensure_data_table(fao_data_raw)
  total_steps <- 2

  # Progress bar identical to reading pipeline, full length
  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    clear = FALSE
  ))

  progressr::with_progress({
    p <- progressr::progressor(along = seq_len(total_steps))

    # 1. Export final dataset
    export_processed_data(
      fao_data_raw,
      config,
      base_name = "fao_data_raw",
      overwrite = overwrite
    )
    p() # increment progress

    # 2. Export unique-column lists
    export_selected_unique_lists(fao_data_raw, config, overwrite)
    p() # increment progress
  })

  invisible(TRUE)
}

# ------------------------------
# 3. Execute automatically
# ------------------------------
run_export_pipeline(fao_data_raw, config, overwrite = TRUE)

# ------------------------------
# End of script
# ------------------------------