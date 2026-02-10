# ============================================================
# Script:  run_pipeline.R
# Purpose: Top-level orchestrator calling general, input, and
#          output pipelines in order.
# ============================================================

source(here::here("R/0-general_pipeline/run_general_pipeline.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)
source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

View(final_dt)