source(here::here("R/0-general_pipeline/00-dependencies.R"), echo = FALSE)
source(here::here("R/0-general_pipeline/01-setup.R"), echo = FALSE)
source(here::here("R/0-general_pipeline/02-helpers.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/10-file_io.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/11-reading.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/12-transform.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/13-validate_log.R"), echo = FALSE)
source(here::here("R/1-import_pipeline/15-output.R"), echo = FALSE)

load_dependencies(required_packages)

test_config <- load_pipeline_config("fao_data_raw")
