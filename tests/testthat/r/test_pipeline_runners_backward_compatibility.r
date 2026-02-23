testthat::test_that("run_general_pipeline preserves signature and return contract", {
  withr::local_options(fao.run_general_pipeline.auto = FALSE)
  source(here::here("R/0-general_pipeline/run_general_pipeline.R"), local = TRUE)

  testthat::expect_identical(
    names(formals(run_general_pipeline)),
    c("dataset_name")
  )
  testthat::expect_identical(
    formals(run_general_pipeline)$dataset_name,
    "fao_data_raw"
  )

  output <- testthat::with_mocked_bindings(
    source_general_scripts = function(script_names) {
      invisible(script_names)
    },
    check_dependencies = function(packages) {
      invisible(character(0))
    },
    load_dependencies = function(packages) {
      invisible(NULL)
    },
    load_pipeline_config = function(dataset_name = "fao_data_raw") {
      list(paths = list(data = list()))
    },
    create_required_directories = function(paths) {
      invisible(TRUE)
    },
    required_packages = "stats",
    .env = environment(run_general_pipeline),
    {
      run_general_pipeline(dataset_name = "fao_data_raw")
    }
  )

  testthat::expect_type(output, "list")
  testthat::expect_true("paths" %in% names(output))
})

testthat::test_that("run_import_pipeline preserves signature and return structure", {
  withr::local_options(fao.run_import_pipeline.auto = FALSE)
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), local = TRUE)

  testthat::expect_identical(names(formals(run_import_pipeline)), c("config"))

  config <- list(paths = list(data = list(imports = list(raw = tempdir()))))

  output <- testthat::with_mocked_bindings(
    discover_files = function(import_folder) {
      data.table::data.table(file_path = "a.xlsx", document = "a.xlsx")
    },
    read_pipeline_files = function(file_list_dt, config) {
      list(
        read_data_list = list(data.table::data.table(x = 1L)),
        errors = character(0)
      )
    },
    transform_files_list = function(file_list_dt, read_data_list, config) {
      list(
        wide_raw = data.table::data.table(x = 1L),
        long_raw = data.table::data.table(document = "a.xlsx", value = "1")
      )
    },
    validate_long_dt = function(long_dt, config) {
      list(data = long_dt, errors = character(0))
    },
    consolidate_audited_dt = function(dt_list, config) {
      list(data = dt_list[[1]], warnings = character(0))
    },
    .env = environment(run_import_pipeline),
    {
      run_import_pipeline(config)
    }
  )

  testthat::expect_type(output, "list")
  testthat::expect_named(output, c("data", "wide_raw", "diagnostics"))
  testthat::expect_named(
    output$diagnostics,
    c("reading_errors", "validation_errors", "warnings")
  )
})

testthat::test_that("run_export_pipeline preserves signature defaults and return structure", {
  withr::local_options(fao.run_export_pipeline.auto = FALSE)

  assign("fao_data_raw", data.frame(x = 1L), envir = .GlobalEnv)
  assign("config", list(), envir = .GlobalEnv)
  on.exit(rm("fao_data_raw", "config", envir = .GlobalEnv), add = TRUE)

  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), local = TRUE)

  testthat::expect_identical(
    names(formals(run_export_pipeline)),
    c("fao_data_raw", "config", "overwrite")
  )
  testthat::expect_identical(formals(run_export_pipeline)$overwrite, TRUE)

  output <- testthat::with_mocked_bindings(
    ensure_data_table = function(df) {
      data.table::as.data.table(df)
    },
    audit_data_output = function(fao_data_raw, config) {
      fao_data_raw
    },
    export_processed_data = function(fao_data_raw, config, base_name = "data_export", overwrite = TRUE) {
      "processed.xlsx"
    },
    export_selected_unique_lists = function(df, config, overwrite = TRUE) {
      "lists.xlsx"
    },
    .env = environment(run_export_pipeline),
    {
      run_export_pipeline(data.frame(x = 1L), list(dummy = TRUE), overwrite = TRUE)
    }
  )

  testthat::expect_type(output, "list")
  testthat::expect_named(output, c("processed_path", "lists_path"))
  testthat::expect_identical(output$processed_path, "processed.xlsx")
  testthat::expect_identical(output$lists_path, "lists.xlsx")
})
