# tests/3-export_pipeline/test-export-data.R
# unit tests for scripts/3-export_pipeline/30-export_data.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "30-export_data.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "31-export_lists.R"), echo = FALSE)


# --- collect_layer_tables_for_export -----------------------------------------

testthat::test_that("collect_layer_tables_for_export auto-detects supported layers", {
  env <- new.env(parent = emptyenv())
  env$demo_raw <- data.frame(a = 1:2)
  env$demo_clean <- data.frame(a = 1:2)
  env$demo_other <- data.frame(a = 1:2)
  env$demo_wide_raw <- data.frame(a = 1:2)

  layer_tables <- collect_layer_tables_for_export(data_objects = NULL, env = env)

  testthat::expect_setequal(names(layer_tables), c("demo_cleaned", "demo_raw"))
  testthat::expect_false("demo_wide_raw" %in% names(layer_tables))
})

testthat::test_that("collect_layer_tables_for_export canonicalizes legacy names", {
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

testthat::test_that("collect_layer_tables_for_export accepts explicit data_objects", {
  data_objects <- list(
    test_raw = data.frame(a = 1:2),
    test_cleaned = data.frame(a = 1:2)
  )

  layer_tables <- collect_layer_tables_for_export(
    data_objects = data_objects,
    env = new.env(parent = emptyenv())
  )

  testthat::expect_true("test_raw"     %in% names(layer_tables))
  testthat::expect_true("test_cleaned" %in% names(layer_tables))
})


# --- canonicalize_layer_object_name ------------------------------------------

testthat::test_that("canonicalize_layer_object_name normalizes legacy suffixes", {
  testthat::expect_identical(canonicalize_layer_object_name("data_clean"), "data_cleaned")
  testthat::expect_identical(canonicalize_layer_object_name("data_harmonize"), "data_harmonized")
  testthat::expect_identical(canonicalize_layer_object_name("data_standardize"), "data_normalized")
})

testthat::test_that("canonicalize_layer_object_name preserves modern names", {
  testthat::expect_identical(canonicalize_layer_object_name("data_raw"), "data_raw")
  testthat::expect_identical(canonicalize_layer_object_name("data_cleaned"), "data_cleaned")
  testthat::expect_identical(canonicalize_layer_object_name("data_harmonized"), "data_harmonized")
})


# --- build_processed_export_path ---------------------------------------------

testthat::test_that("build_processed_export_path generates correct naming", {
  config <- build_test_config()

  path <- build_processed_export_path(config, "dataset_clean")

  testthat::expect_match(basename(path), "^dataset_clean\\.xlsx$")
})


# --- write_processed_table_excel ---------------------------------------------

testthat::test_that("write_processed_table_excel writes valid xlsx", {
  root_dir <- build_temp_dir("fao-write-table-")
  file_path <- file.path(root_dir, "output.xlsx")

  dt <- data.table::data.table(
    country = c("Japan", "France"),
    value = c("100", "200")
  )

  write_processed_table_excel(dt, file_path)

  testthat::expect_true(file.exists(file_path))

  # verify the file can be read back
  read_back <- openxlsx::read.xlsx(file_path)
  testthat::expect_equal(nrow(read_back), 2L)
})


# --- export_processed_data --------------------------------------------------

testthat::test_that("export_processed_data writes workbooks for detected layers", {
  config <- build_test_config()

  data_objects <- list(
    test_raw = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2")
    ),
    test_cleaned = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2")
    )
  )

  paths <- export_processed_data(
    config = config,
    data_objects = data_objects,
    overwrite = TRUE,
    env = new.env(parent = emptyenv())
  )

  testthat::expect_true(is.character(paths))
  testthat::expect_true(length(paths) >= 1L)
  testthat::expect_true(all(file.exists(paths)))
})
