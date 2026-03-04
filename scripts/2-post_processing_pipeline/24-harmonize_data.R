# script: compatibility wrapper for harmonize stage functions
# description: sources unified clean/harmonize implementation to preserve
# legacy script path compatibility.

source(here::here(
  "scripts",
  "2-post_processing_pipeline",
  "22-clean_harmonize_data.R"
), echo = FALSE)
