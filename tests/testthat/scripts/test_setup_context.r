options(
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_clean_harmonize_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "21-post_processing_utilities.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "22-clean_data.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "23-standardize_units.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "24-standardize_data.R"), echo = FALSE)
source(here::here("scripts", "2-post_processing_pipeline", "25-post_processing_diagnostics.R"), echo = FALSE)
