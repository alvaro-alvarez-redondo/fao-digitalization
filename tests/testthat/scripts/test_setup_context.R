options(
  whep.run_postpro_pipeline.auto = FALSE
)

source(
  here::here(
    "r",
    "0-general_pipeline",
    "01-setup.R"
  ),
  echo = FALSE
)

source(
  here::here(
    "r",
    "2-postpro_pipeline",
    "25-postpro_diagnostics.R"
  ),
  echo = FALSE
)
