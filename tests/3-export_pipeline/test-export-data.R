# tests/3-export_pipeline/test-export-data.R
# unit tests for scripts/3-export_pipeline/30-export_data.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "30-export_data.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "31-export_lists.R"), echo = FALSE)


# --- collect_layer_tables_for_export -----------------------------------------

testthat::test_that("collect_layer_tables_for_export auto-detects strict layers", {
  env <- new.env(parent = emptyenv())
  env$demo_raw <- data.frame(a = 1:2)
  env$demo_cleaned <- data.frame(a = 1:2)
  env$demo_other <- data.frame(a = 1:2)
  env$demo_wide_raw <- data.frame(a = 1:2)

  layer_tables <- collect_layer_tables_for_export(data_objects = NULL, env = env)

  testthat::expect_setequal(names(layer_tables), c("demo_cleaned", "demo_raw"))
  testthat::expect_false("demo_wide_raw" %in% names(layer_tables))
})

testthat::test_that("collect_layer_tables_for_export rejects legacy suffixes", {
  env <- new.env(parent = emptyenv())
  env$fao_data_harmonized <- data.frame(a = 1:2)
  env$fao_data_clean <- data.frame(a = 1:2)
  env$fao_data_harmonize <- data.frame(a = 1:2)
  env$fao_data_standardize <- data.frame(a = 1:2)

  layer_tables <- collect_layer_tables_for_export(data_objects = NULL, env = env)

  testthat::expect_setequal(names(layer_tables), c("fao_data_harmonized"))
  testthat::expect_false("fao_data_clean" %in% names(layer_tables))
  testthat::expect_false("fao_data_harmonize" %in% names(layer_tables))
  testthat::expect_false("fao_data_standardize" %in% names(layer_tables))
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


# --- build_processed_export_path ---------------------------------------------

testthat::test_that("build_processed_export_path generates correct naming", {
  config <- build_test_config()

  path <- build_processed_export_path(config, "dataset_harmonized")

  testthat::expect_match(basename(path), "^dataset_harmonized\\.xlsx$")
})


# --- write_processed_table_fast ----------------------------------------------

testthat::test_that("write_processed_table_fast writes valid xlsx with correct content", {
  root_dir <- build_temp_dir("fao-write-table-")
  file_path <- file.path(root_dir, "output.xlsx")

  dt <- data.table::data.table(
    country = c("Japan", "France"),
    value = c("100", "200")
  )

  write_processed_table_fast(dt, file_path)

  testthat::expect_true(file.exists(file_path))

  # verify the file can be read back with correct content
  read_back <- readxl::read_excel(file_path)
  testthat::expect_equal(nrow(read_back), 2L)
  testthat::expect_equal(colnames(read_back), c("country", "value"))
  testthat::expect_equal(read_back$country, c("Japan", "France"))
})

testthat::test_that("write_processed_table_fast respects overwrite flag", {
  root_dir <- build_temp_dir("fao-write-overwrite-")
  file_path <- file.path(root_dir, "output.xlsx")

  dt <- data.table::data.table(a = 1:2)
  write_processed_table_fast(dt, file_path)

  testthat::expect_error(
    write_processed_table_fast(dt, file_path, overwrite = FALSE),
    "overwrite"
  )
})


# --- export_processed_data --------------------------------------------------

testthat::test_that("export_processed_data writes workbooks only for harmonized layer", {
  config <- build_test_config()

  data_objects <- list(
    test_raw = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2")
    ),
    test_cleaned = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2")
    ),
    test_harmonized = data.table::data.table(
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
  testthat::expect_equal(length(paths), 1L)
  testthat::expect_true(all(file.exists(paths)))
  testthat::expect_true(grepl("harmonized", names(paths)))
})

testthat::test_that("export_processed_data errors when no harmonized layer present", {
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

  testthat::expect_error(
    export_processed_data(
      config = config,
      data_objects = data_objects,
      overwrite = TRUE,
      env = new.env(parent = emptyenv())
    ),
    "harmonized"
  )
})
