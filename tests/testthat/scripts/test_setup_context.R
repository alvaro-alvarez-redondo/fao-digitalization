options(
  fao.run_post_processing_pipeline.auto = FALSE
)

source(
  here::here(
    "scripts",
    "0-general_pipeline",
    "01-setup.R"
  ),
  echo = FALSE
)

source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "25-post_processing_diagnostics.R"
  ),
  echo = FALSE
)
