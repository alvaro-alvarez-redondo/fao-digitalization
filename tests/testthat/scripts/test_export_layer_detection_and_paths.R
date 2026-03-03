options(
  fao.run_export_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "30-export_data.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "31-export_lists.R"), echo = FALSE)

build_export_test_config <- function() {
  list(
    paths = list(
      data = list(
        exports = list(
          processed = file.path("data", "3-export", "processed_data"),
          lists = file.path("data", "3-export", "lists")
        )
      )
    )
  )
}

testthat::test_that("collect_layer_tables_for_export auto-detects supported layers", {
  env <- new.env(parent = emptyenv())
  env$demo_raw <- data.frame(a = 1:2)
  env$demo_clean <- data.frame(a = 1:2)
  env$demo_other <- data.frame(a = 1:2)

  layer_tables <- collect_layer_tables_for_export(data_objects = NULL, env = env)

  testthat::expect_setequal(names(layer_tables), c("demo_clean", "demo_raw"))
})

testthat::test_that("build export paths follow required naming conventions", {
  config <- build_export_test_config()

  processed_path <- build_processed_export_path(config, "dataset_clean")
  lists_path <- build_lists_export_path(config, "dataset_clean")

  testthat::expect_match(basename(processed_path), "^dataset_clean\\.xlsx$")
  testthat::expect_match(basename(lists_path), "^dataset_clean_lists\\.xlsx$")
})


testthat::test_that("collect_layer_tables_for_export canonicalizes legacy names and drops post_processed", {
  env <- new.env(parent = emptyenv())
  env$fao_data_raw <- data.frame(a = 1:2)
  env$fao_data_clean <- data.frame(a = 1:2)
  env$fao_data_harmonize <- data.frame(a = 1:2)
  env$fao_data_standardize <- data.frame(a = 1:2)
  env$fao_data_post_processed <- data.frame(a = 1:2)

  layer_tables <- collect_layer_tables_for_export(data_objects = NULL, env = env)

  testthat::expect_setequal(
    names(layer_tables),
    c("fao_data_raw", "fao_data_cleaned", "fao_data_harmonized", "fao_data_normalized")
  )
  testthat::expect_false("fao_data_post_processed" %in% names(layer_tables))
})
