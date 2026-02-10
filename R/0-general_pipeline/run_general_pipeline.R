# ============================================================
# Script:  run_general_pipeline.R
# Purpose: Setup project environment, load dependencies,
#          and initialize configuration and directories.
# ============================================================

# ------------------------------
# Source general scripts
# ------------------------------
general_scripts <- c(
  "00-dependencies.R",
  "01-setup.R",
  "02-helpers.R"
)

purrr::walk(
  general_scripts,
  ~ source(here::here("R/0-general_pipeline", .x), echo = FALSE)
)

# ------------------------------
# Load/install dependencies
# ------------------------------
check_dependencies(required_packages)
load_dependencies(required_packages)

# ------------------------------
# Load pipeline configuration
# ------------------------------
config <- load_pipeline_config()
create_required_directories(config$paths)

# ------------------------------
# End of general setup
# ------------------------------
