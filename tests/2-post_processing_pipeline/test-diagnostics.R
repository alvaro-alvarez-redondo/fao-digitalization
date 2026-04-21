# tests/2-post_processing_pipeline/test-diagnostics.R
# unit tests for R/2-post_processing_pipeline/25-post_processing_diagnostics.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here(
    "r",
    "2-post_processing_pipeline",
    "21-post_processing_utilities.R"
  ),
  echo = FALSE
)
source(
  here::here(
    "r",
    "2-post_processing_pipeline",
    "23-post_processing_rule_engine.R"
  ),
  echo = FALSE
)
source(
  here::here(
    "r",
    "2-post_processing_pipeline",
    "25-post_processing_diagnostics.R"
  ),
  echo = FALSE
)


# --- collect_post_processing_preflight ---------------------------------------

testthat::test_that("preflight flags invalid file naming patterns", {
  config <- build_test_config()
  dir.create(
    file.path(config$paths$data$audit$audit_root_dir, "templates"),
    recursive = TRUE,
    showWarnings = FALSE
  )
  dir.create(
    file.path(
      config$paths$data$audit$audit_root_dir,
      "post_processing_diagnostics"
    ),
    recursive = TRUE,
    showWarnings = FALSE
  )

  file.create(file.path(
    config$paths$data$imports$cleaning,
    "bad_clean_name.xlsx"
  ))
  file.create(file.path(
    config$paths$data$imports$harmonization,
    "bad_harmonize_name.xlsx"
  ))

  result <- collect_post_processing_preflight(
    config = config,
    dataset_columns = c("unit", "value", "product")
  )

  testthat::expect_false(result$checks$cleaning_pattern_ok)
  testthat::expect_false(result$checks$harmonize_pattern_ok)
  testthat::expect_true(any(grepl("clean stage", result$issues, fixed = TRUE)))
  testthat::expect_true(any(grepl(
    "harmonize stage",
    result$issues,
    fixed = TRUE
  )))
})

testthat::test_that("preflight detects column convention mismatch", {
  config <- build_test_config()
  dir.create(
    file.path(config$paths$data$audit$audit_root_dir, "templates"),
    recursive = TRUE,
    showWarnings = FALSE
  )
  dir.create(
    file.path(
      config$paths$data$audit$audit_root_dir,
      "post_processing_diagnostics"
    ),
    recursive = TRUE,
    showWarnings = FALSE
  )

  file.create(file.path(config$paths$data$imports$cleaning, "clean_rules.xlsx"))
  file.create(file.path(
    config$paths$data$imports$harmonization,
    "harmonize_rules.xlsx"
  ))

  result <- collect_post_processing_preflight(
    config = config,
    dataset_columns = c("unit", "value", "item")
  )

  testthat::expect_false(result$passed)
  testthat::expect_true(any(grepl(
    "missing expected columns",
    result$issues,
    fixed = TRUE
  )))
})

testthat::test_that("assert_post_processing_preflight aborts with stage details", {
  bad_result <- list(
    passed = FALSE,
    issues = c(
      "[clean stage] missing file",
      "[harmonize stage] missing template"
    ),
    checks = list()
  )

  testthat::expect_error(
    assert_post_processing_preflight(bad_result),
    regexp = "Post-processing preflight checks failed"
  )
})


# --- summarize_stage_rules ---------------------------------------------------

testthat::test_that("summarize_stage_rules aggregates audit records", {
  audit_dt <- data.table::data.table(
    loop = c(1L, 2L),
    rule_file_identifier = c("clean_rules.csv", "clean_rules.csv"),
    value_source_raw = c("wheat", "rice"),
    value_source_result = c("wheat_clean", "rice_clean"),
    value_target_raw = c("kg", "kg"),
    value_target_result = c("kilogram", "gram"),
    column_source = c("product", "product"),
    column_target = c("unit", "variable"),
    affected_rows = c(5L, 3L)
  )

  result <- summarize_stage_rules(audit_dt, stage_name = "clean")

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true("loop" %in% names(result))
  testthat::expect_true("affected_rows" %in% names(result))
  testthat::expect_false("execution_stage" %in% names(result))
  testthat::expect_identical(names(result)[1:2], c("loop", "affected_rows"))
  testthat::expect_identical(sort(unique(result$loop)), c(1L, 2L))
  testthat::expect_false(any(is.na(result$value_source)))
  testthat::expect_true(all(
    result$value_source %in% c("wheat_clean", "rice_clean")
  ))
  testthat::expect_false(any(is.na(result$value_target)))
  testthat::expect_true(all(result$value_target %in% c("kilogram", "gram")))
  testthat::expect_true(nrow(result) >= 1L)
})


# --- build_post_processing_diagnostics ---------------------------------------

testthat::test_that("build_post_processing_diagnostics creates stage summaries", {
  clean_audit_dt <- data.table::data.table(
    loop = 1L,
    rule_file_identifier = "clean_rules.csv",
    value_source_raw = "wheat",
    value_source = "wheat_clean",
    value_target_raw = "kg",
    value_target = "kilogram",
    column_source = "product",
    column_target = "unit",
    affected_rows = 5L
  )

  harmonize_audit_dt <- data.table::data.table(
    loop = 1L,
    rule_file_identifier = "harmonize_rules.csv",
    value_source_raw = "usa",
    value_source = "united_states",
    value_target_raw = "usa",
    value_target = "united states",
    column_source = "country",
    column_target = "country",
    affected_rows = 2L
  )

  result <- build_post_processing_diagnostics(
    clean_audit_dt = clean_audit_dt,
    harmonize_audit_dt = harmonize_audit_dt
  )

  testthat::expect_true(is.list(result))
  testthat::expect_true("clean_rule_summary" %in% names(result))
  testthat::expect_true("harmonize_rule_summary" %in% names(result))
})


# --- persist_post_processing_audit -------------------------------------------

testthat::test_that("persist_post_processing_audit writes overwrite subset diagnostics excel", {
  config <- build_test_config()
  dir.create(
    file.path(
      config$paths$data$audit$audit_root_dir,
      "post_processing_diagnostics"
    ),
    recursive = TRUE,
    showWarnings = FALSE
  )

  clean_audit <- data.table::data.table(
    loop = integer(0),
    rule_file_identifier = character(0),
    column_source = character(0),
    value_source_raw = character(0),
    value_source = character(0),
    column_target = character(0),
    value_target_raw = character(0),
    value_target = character(0),
    affected_rows = integer(0)
  )
  harmonize_audit <- data.table::data.table(
    loop = integer(0),
    rule_file_identifier = character(0),
    column_source = character(0),
    value_source_raw = character(0),
    value_source = character(0),
    column_target = character(0),
    value_target_raw = character(0),
    value_target = character(0),
    affected_rows = integer(0)
  )
  standardize_rows <- data.table::data.table(
    area = c("CountryA", "CountryB"),
    product = c("wheat", "rice"),
    unit = c("t", "t"),
    value = c(100, 200)
  )

  final_stage_dt <- data.table::data.table(
    country = c("CountryA", "CountryB"),
    notes = c("note a", "note b"),
    footnotes = c("f1", "f2")
  )

  overwrite_events_dt <- data.table::data.table(
    dataset_name = "demo",
    execution_stage = "harmonize",
    rule_file_identifier = "harmonize_rules.xlsx",
    column_source = "country",
    column_target = "notes",
    row_id = 2L,
    candidate_count = 2L,
    unique_candidate_count = 2L,
    selected_value = "note b",
    candidate_values = "note a; note b"
  )

  output_paths <- persist_post_processing_audit(
    clean_audit_dt = clean_audit,
    harmonize_audit_dt = harmonize_audit,
    standardize_rows_dt = standardize_rows,
    final_stage_dt = final_stage_dt,
    last_rule_wins_overwrites_dt = overwrite_events_dt,
    config = config
  )

  testthat::expect_true("clean_harmonize_audit" %in% names(output_paths))
  testthat::expect_true(file.exists(output_paths[["clean_harmonize_audit"]]))
  testthat::expect_true("aggregate_standardized_rows" %in% names(output_paths))
  testthat::expect_true(file.exists(output_paths[[
    "aggregate_standardized_rows"
  ]]))
  testthat::expect_true("last_rule_wins_overwrites" %in% names(output_paths))
  testthat::expect_true(file.exists(output_paths[[
    "last_rule_wins_overwrites"
  ]]))
  testthat::expect_identical(
    readxl::excel_sheets(output_paths[["clean_harmonize_audit"]]),
    c("clean", "harmonize")
  )
  testthat::expect_identical(
    readxl::excel_sheets(output_paths[["aggregate_standardized_rows"]]),
    "aggregate_standardized_rows"
  )
  testthat::expect_identical(
    readxl::excel_sheets(output_paths[["last_rule_wins_overwrites"]]),
    "last_rule_wins_overwrites"
  )

  clean_summary <- readxl::read_excel(
    output_paths[["clean_harmonize_audit"]],
    sheet = "clean"
  )
  harmonize_summary <- readxl::read_excel(
    output_paths[["clean_harmonize_audit"]],
    sheet = "harmonize"
  )

  testthat::expect_true("loop" %in% colnames(clean_summary))
  testthat::expect_true("loop" %in% colnames(harmonize_summary))
  testthat::expect_false("execution_stage" %in% colnames(clean_summary))
  testthat::expect_false("execution_stage" %in% colnames(harmonize_summary))
  testthat::expect_identical(
    colnames(clean_summary)[1:2],
    c("loop", "affected_rows")
  )
  testthat::expect_identical(
    colnames(harmonize_summary)[1:2],
    c("loop", "affected_rows")
  )
  required_summary_columns <- c(
    "column_source",
    "value_source_raw",
    "value_source",
    "column_target",
    "value_target_raw",
    "value_target"
  )
  testthat::expect_true(all(
    required_summary_columns %in% colnames(clean_summary)
  ))
  testthat::expect_true(all(
    required_summary_columns %in% colnames(harmonize_summary)
  ))
})
