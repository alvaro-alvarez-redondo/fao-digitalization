# ============================================================
# Script:  00-dependencies.R
# Purpose: Declare, install (if needed), and load all required
#          packages for the project.
# ============================================================

# ------------------------------
# Required packages
# ------------------------------
required_packages <- c(
  "tidyverse", # Core data manipulation and functional programming
  "data.table", # High-performance data handling
  "readxl", # Read Excel files (.xlsx)
  "janitor", # Clean and standardize column names
  "openxlsx", # Write Excel files
  "fs", # File system operations
  "here", # Project-relative paths
  "stringi", # String transliteration and ASCII checks
  "progressr", # Progress bars for apply functions
  "renv", # Project-specific package management
  "testthat", # Unit testing framework
  "checkmate", # Assertions and checks
  "progress" # Progress bars for loops and apply functions
)

# ------------------------------
# Function. Find missing packages
# ------------------------------
check_dependencies <- function(packages) {
  missing <- setdiff(packages, rownames(installed.packages()))
  if (length(missing) > 0) {
    message("Installing missing packages: ", paste(missing, collapse = ", "))
    install.packages(missing)
  }
}

load_dependencies <- function(packages) {
  purrr::walk(packages, function(package_name) {
    suppressPackageStartupMessages(
      library(package_name, character.only = TRUE)
    )
  })
}

# ------------------------------
# End of script
# ------------------------------
