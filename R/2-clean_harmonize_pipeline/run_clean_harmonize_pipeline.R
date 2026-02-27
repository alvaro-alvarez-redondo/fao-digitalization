# script: backward-compatible clean-harmonize runner shim
# description: delegate execution to the post-processing pipeline runner.

source(
  here::here("R", "2-post_processing_pipeline", "run_post_processing_pipeline.R"),
  echo = FALSE
)
