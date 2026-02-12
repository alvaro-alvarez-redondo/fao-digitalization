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
# Function. Run export pipeline
# ------------------------------
run_export_pipeline <- function(fao_data_raw, config, overwrite = TRUE) {
  fao_data_raw <- ensure_data_table(fao_data_raw)
  total_steps <- 2

  progressr::handlers(progressr::handler_txtprogressbar(
    style = 3,
    clear = FALSE
  ))

  progressr::with_progress({
    progress <- progressr::progressor(along = seq_len(total_steps))

    processed_path <- export_processed_data(
      fao_data_raw,
      config,
      base_name = "fao_data_raw",
      overwrite = overwrite
    )
    progress()

    lists_path <- export_selected_unique_lists(fao_data_raw, config, overwrite)
    progress()

    list(processed_path = processed_path, lists_path = lists_path)
  })
}

# ------------------------------
# Execute automatically
# ------------------------------
if (!exists("fao_data_raw")) {
  stop(
    "fao_data_raw not found in the environment. make sure the import pipeline has run."
  )
}

export_paths <- run_export_pipeline(fao_data_raw, config, overwrite = TRUE)

# ------------------------------
# End of script
# ------------------------------
