# script: compatibility shim for modular post-processing functions
# description: sources modular post-processing stage scripts.

purrr::walk(
  c(
    "20-data_audit.R",
    "21-io_and_utilities.R",
    "22-clean_data.R",
    "23-standardize_data.R",
    "24-standardize_units.R"
  ),
  \(script_name) {
    source(here::here("R", "2-post_processing_pipeline", script_name), echo = FALSE)
  }
)
