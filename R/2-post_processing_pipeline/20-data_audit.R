# script: backward-compatible data audit shim
# description: delegate data audit functions to post-processing pipeline.

source(
  here::here("R", "2-post_processing_pipeline", "20-data_audit.R"),
  echo = FALSE
)
