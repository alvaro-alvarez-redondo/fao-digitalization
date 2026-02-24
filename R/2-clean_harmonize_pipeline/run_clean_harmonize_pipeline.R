# script: run cleaning and harmonization pipeline
# description: source cleaning/harmonization functions and execute the stage
# between import and export pipelines.

source(here::here("R", "2-clean_harmonize_pipeline", "20-cleaning_harmonization.R"), echo = FALSE)

if (!exists("fao_data_raw")) {
  cli::cli_abort("fao_data_raw not found in the environment. make sure the import pipeline has run.")
}

if (!exists("config")) {
  cli::cli_abort("config not found in the environment. make sure configuration has been loaded.")
}

if (isTRUE(getOption("fao.run_clean_harmonize_pipeline.auto", TRUE))) {
  fao_data_harmonized <- run_clean_harmonize_pipeline(
    raw_dt = fao_data_raw,
    config = config,
    aggregation = TRUE
  )

  fao_data_raw <- fao_data_harmonized
}
