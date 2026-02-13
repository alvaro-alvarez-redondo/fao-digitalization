# script: run_pipeline.r
# description: run general import and export pipelines in sequence for the full workflow.

source(here::here("R/0-general_pipeline/run_general_pipeline.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)
source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

if (interactive() && exists("fao_data_raw")) {
  utils::View(fao_data_raw)
}
