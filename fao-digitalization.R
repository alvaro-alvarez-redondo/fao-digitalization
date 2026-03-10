profvis::profvis(
  {
    source(here::here("scripts/run_pipeline.R"), echo = FALSE)
  },
  prof_output = "."
)
