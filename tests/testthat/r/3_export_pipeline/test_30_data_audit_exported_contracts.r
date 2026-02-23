source(here::here("R/3-export_pipeline/30-data_audit.R"), echo = FALSE)

testthat::test_that("prepare_audit_root deletes existing directory and returns TRUE", {
  audit_root_dir <- fs::path(withr::local_tempdir(), "audit-root")
  fs::dir_create(audit_root_dir)

  output_flag <- prepare_audit_root(audit_root_dir)

  testthat::expect_true(isTRUE(output_flag))
  testthat::expect_false(fs::dir_exists(audit_root_dir))
})

testthat::test_that("prepare_audit_root returns FALSE when directory does not exist", {
  audit_root_dir <- fs::path(withr::local_tempdir(), "audit-root-missing")

  output_flag <- prepare_audit_root(audit_root_dir)

  testthat::expect_identical(output_flag, FALSE)
})

testthat::test_that("prepare_audit_root aborts on invalid path input", {
  testthat::expect_error(
    prepare_audit_root(""),
    class = "rlang_error"
  )
})

testthat::test_that("resolve_audit_output_paths returns stable schema", {
  output_paths <- resolve_audit_output_paths(
    audit_root_dir = "data/audit/fao_data_raw",
    audit_file_name = "fao_data_raw_audit.xlsx",
    mirror_dir_name = "raw_imports_mirror"
  )

  testthat::expect_named(
    output_paths,
    c("audit_root_dir", "audit_file_path", "mirror_dir_path")
  )
  testthat::expect_identical(output_paths$audit_root_dir, "data/audit/fao_data_raw")
  testthat::expect_match(output_paths$audit_file_path, "fao_data_raw_audit\\.xlsx$")
  testthat::expect_match(output_paths$mirror_dir_path, "raw_imports_mirror$")
})

testthat::test_that("resolve_audit_columns_by_type preserves explicit mapping", {
  config <- load_pipeline_config("fao_data_raw")
  config$audit_columns_by_type <- list(
    character_non_empty = c("continent", "country"),
    numeric_string = c("value")
  )

  audit_map <- resolve_audit_columns_by_type(config)

  testthat::expect_type(audit_map, "list")
  testthat::expect_identical(audit_map$character_non_empty, c("continent", "country"))
  testthat::expect_identical(audit_map$numeric_string, "value")
})

testthat::test_that("resolve_audit_columns_by_type fallback keeps backward-compatible defaults", {
  config <- load_pipeline_config("fao_data_raw")
  config$audit_columns_by_type <- NULL

  audit_map <- resolve_audit_columns_by_type(config)

  testthat::expect_identical(audit_map$character_non_empty, unique(config$audit_columns))
  testthat::expect_identical(audit_map$numeric_string, intersect("value", config$column_order))
})

testthat::test_that("resolve_audit_columns_by_type aborts on invalid config", {
  testthat::expect_error(
    resolve_audit_columns_by_type(list()),
    class = "rlang_error"
  )
})
