# script: compatibility shim for modular clean-harmonize functions
# description: sources modular clean-harmonize stage scripts.

purrr::walk(
  c(
    "21-common_clean_harmonize_functions.R",
    "22-run_cleaning_stage.R",
    "24-run_general_harmonization_stage.R",
    "23-run_numeric_harmonization_stage.R"
  ),
  \(script_name) {
    source(here::here("R", "2-clean_harmonize_pipeline", script_name), echo = FALSE)
  }
)
