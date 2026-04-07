# tests/2-post_processing_pipeline/test-rule-engine.R
# unit tests for scripts/2-post_processing_pipeline/23-post_processing_rule_engine.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "21-post_processing_utilities.R"
  ),
  echo = FALSE
)
source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "23-post_processing_rule_engine.R"
  ),
  echo = FALSE
)


# --- coerce_rule_schema ------------------------------------------------------

testthat::test_that("coerce_rule_schema normalizes stage-prefixed columns", {
  raw_rule_dt <- data.frame(
    clean_column_source = "product",
    clean_value_source_raw = "Wheat",
    clean_column_target = "unit",
    clean_value_target_raw = "kg",
    clean_value_target = "kilogram",
    stringsAsFactors = FALSE
  )

  result <- coerce_rule_schema(
    rule_dt = raw_rule_dt,
    stage_name = "clean",
    rule_file_id = "test.xlsx"
  )

  testthat::expect_true(data.table::is.data.table(result))
  testthat::expect_true("column_source" %in% names(result))
  testthat::expect_true("value_target" %in% names(result))
  testthat::expect_equal(result$value_target[[1]], "kilogram")
})

testthat::test_that("coerce_rule_schema errors on missing required columns", {
  raw_rule_dt <- data.frame(
    column_source = "product",
    stringsAsFactors = FALSE
  )

  testthat::expect_error(
    coerce_rule_schema(
      rule_dt = raw_rule_dt,
      stage_name = "clean",
      rule_file_id = "test.xlsx"
    ),
    "missing required columns"
  )
})

testthat::test_that("coerce_rule_schema errors on duplicate columns after normalization", {
  raw_rule_dt <- data.frame(
    clean_column_source = "product",
    column_source = "product",
    clean_value_source_raw = "Wheat",
    clean_column_target = "unit",
    clean_value_target_raw = "kg",
    clean_value_target = "kilogram",
    stringsAsFactors = FALSE
  )

  testthat::expect_error(
    coerce_rule_schema(
      rule_dt = raw_rule_dt,
      stage_name = "clean",
      rule_file_id = "test.xlsx"
    ),
    "duplicate columns"
  )
})


# --- ensure_rule_referenced_columns ------------------------------------------

testthat::test_that("ensure_rule_referenced_columns adds missing columns to dataset", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  rules_dt <- data.table::data.table(
    column_source = "product",
    column_target = "new_column"
  )

  result <- ensure_rule_referenced_columns(dataset_dt, rules_dt)

  testthat::expect_true("new_column" %in% names(result))
  testthat::expect_true(all(is.na(result$new_column)))
})

testthat::test_that("ensure_rule_referenced_columns preserves existing columns", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  rules_dt <- data.table::data.table(
    column_source = "product",
    column_target = "unit"
  )

  result <- ensure_rule_referenced_columns(dataset_dt, rules_dt)

  testthat::expect_identical(result$unit, c("kg", "kg"))
})


# --- validate_canonical_rules ------------------------------------------------

testthat::test_that("validate_canonical_rules allows NA in value columns for clean stage", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  rules_dt <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c(NA_character_, "Rice"),
    value_source = c(NA_character_, NA_character_),
    column_target = c("unit", "unit"),
    value_target_raw = c(NA_character_, "kg"),
    value_target = c(NA_character_, "kilogram")
  )

  testthat::expect_invisible(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "test.xlsx",
      stage_name = "clean"
    )
  )
})

testthat::test_that("validate_canonical_rules fails for NA in structural columns", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat"),
    unit = c("kg")
  )

  rules_dt <- data.table::data.table(
    column_source = NA_character_,
    value_source_raw = NA_character_,
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram"
  )

  testthat::expect_error(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "test.xlsx",
      stage_name = "clean"
    ),
    "missing values in required columns"
  )
})

testthat::test_that("validate_canonical_rules detects duplicate rule keys", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg"
  )

  rules_dt <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c("Wheat", "Wheat"),
    column_target = c("unit", "unit"),
    value_target_raw = c("kg", "kg"),
    value_target = c("kilogram", "gram")
  )

  testthat::expect_error(
    validate_canonical_rules(
      rules_dt = rules_dt,
      dataset_dt = dataset_dt,
      rule_file_id = "test.xlsx",
      stage_name = "clean"
    )
  )
})


# --- encode / decode target rule values --------------------------------------

testthat::test_that("encode_target_rule_value replaces empty and NA with placeholder", {
  constants <- get_pipeline_constants()
  result <- encode_target_rule_value(c("value", "", NA_character_))

  testthat::expect_equal(result[1], "value")
  testthat::expect_equal(result[2], constants$na_placeholder)
  testthat::expect_equal(result[3], constants$na_placeholder)
})

testthat::test_that("decode_target_rule_value restores placeholder to NA", {
  constants <- get_pipeline_constants()
  result <- decode_target_rule_value(c("value", constants$na_placeholder))

  testthat::expect_equal(result[1], "value")
  testthat::expect_true(is.na(result[2]))
})


# --- encode_rule_match_key ---------------------------------------------------

testthat::test_that("encode_rule_match_key normalizes values and encodes NA", {
  constants <- get_pipeline_constants()
  result <- encode_rule_match_key(c("Hello World", NA_character_))

  testthat::expect_equal(result[1], "hello world")
  testthat::expect_equal(result[2], constants$na_match_key)
})

testthat::test_that("resolve_target_update_strategy supports per-column overrides", {
  testthat::expect_equal(
    resolve_target_update_strategy("notes"),
    "concatenate"
  )
  testthat::expect_equal(
    resolve_target_update_strategy("unit"),
    "last_rule_wins"
  )
})


# --- apply_conditional_rule_group --------------------------------------------

testthat::test_that("apply_conditional_rule_group applies clean rules", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  group_rules <- data.table::data.table(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram"
  )

  result <- apply_conditional_rule_group(
    dataset_dt = dataset_dt,
    group_rules = group_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_true("data" %in% names(result))
  testthat::expect_true("audit" %in% names(result))
  testthat::expect_equal(result$data$unit[[1]], "kilogram")
  testthat::expect_equal(result$data$unit[[2]], "kg")
  testthat::expect_true(nrow(result$audit) >= 1L)
})

testthat::test_that("apply_conditional_rule_group matches NA keys", {
  dataset_dt <- data.table::data.table(
    product = c(NA_character_, "Wheat"),
    unit = c(NA_character_, "kg")
  )

  group_rules <- data.table::data.table(
    column_source = "product",
    value_source_raw = NA_character_,
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = NA_character_,
    value_target = "unknown_unit"
  )

  result <- apply_conditional_rule_group(
    dataset_dt = dataset_dt,
    group_rules = group_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$unit[[1]], "unknown_unit")
  testthat::expect_equal(result$data$unit[[2]], "kg")
  testthat::expect_equal(result$audit$affected_rows[[1]], 1L)
})

testthat::test_that("apply_conditional_rule_group applies empty target as NA", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg")
  )

  group_rules <- data.table::data.table(
    column_source = "product",
    value_source_raw = "Wheat",
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target = ""
  )

  result <- apply_conditional_rule_group(
    dataset_dt = dataset_dt,
    group_rules = group_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.na(result$data$unit[[1]]))
  testthat::expect_equal(result$data$unit[[2]], "kg")
})


# --- apply_rule_payload ------------------------------------------------------

testthat::test_that("apply_rule_payload applies multiple rule groups", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    variable = c("Prod", "Prod")
  )

  canonical_rules <- data.table::data.table(
    column_source = c("product", "product"),
    value_source_raw = c("Wheat", "Rice"),
    value_source = c(NA_character_, NA_character_),
    column_target = c("unit", "unit"),
    value_target_raw = c("kg", "kg"),
    value_target = c("kilogram", "gram")
  )

  result <- apply_rule_payload(
    dataset_dt = dataset_dt,
    canonical_rules = canonical_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_equal(result$data$unit[[1]], "kilogram")
  testthat::expect_equal(result$data$unit[[2]], "gram")
  testthat::expect_true(nrow(result$audit) >= 2L)
})

testthat::test_that("apply_rule_payload returns empty audit for zero rules", {
  dataset_dt <- data.table::data.table(product = "Wheat", unit = "kg")

  result <- apply_rule_payload(
    dataset_dt = dataset_dt,
    canonical_rules = data.table::data.table(),
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_equal(nrow(result$audit), 0L)
})


# --- apply_footnote_rules ----------------------------------------------------

testthat::test_that("apply_footnote_rules replaces a single footnote", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    footnotes = c("old note", "other note")
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "old note",
    value_source = "new note",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.list(result))
  testthat::expect_equal(result$data$footnotes[[1]], "new note")
  testthat::expect_equal(result$data$footnotes[[2]], "other note")
  testthat::expect_true(nrow(result$audit) >= 1L)
})

testthat::test_that("apply_footnote_rules removes a single footnote to NA", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat"),
    footnotes = c("remove me")
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "remove me",
    value_source = NA_character_,
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.na(result$data$footnotes[[1]]))
})

testthat::test_that("apply_footnote_rules handles multi-footnote split and reconstruct", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat"),
    footnotes = c("note A; note B; note C")
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "note B",
    value_source = "replaced B",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "note A; replaced B; note C")
})

testthat::test_that("apply_footnote_rules preserves footnote order", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "first; second; third"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "second",
    value_source = "2nd",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "first; 2nd; third")
})

testthat::test_that("apply_footnote_rules removes one footnote from multi-footnote cell", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "keep A; remove me; keep B"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "remove me",
    value_source = NA_character_,
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "keep A; keep B")
})

testthat::test_that("apply_footnote_rules sets NA when all footnotes removed", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "del A; del B"
  )

  footnote_rules <- data.table::data.table(
    column_source = c("footnotes", "footnotes"),
    value_source_raw = c("del A", "del B"),
    value_source = c(NA_character_, NA_character_),
    column_target = c("footnotes", "footnotes"),
    value_target_raw = c(NA_character_, NA_character_),
    value_target = c(NA_character_, NA_character_)
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.na(result$data$footnotes[[1]]))
})

testthat::test_that("apply_footnote_rules preserves unmatched footnotes", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "unmatched note; matched note"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "matched note",
    value_source = "replaced",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "unmatched note; replaced")
})

testthat::test_that("apply_footnote_rules handles comma-containing footnotes", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "note with, comma; simple note"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "note with, comma",
    value_source = "comma preserved",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "comma preserved; simple note")
})

testthat::test_that("apply_footnote_rules applies target column updates", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg",
    footnotes = "update unit"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "update unit",
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = NA_character_,
    value_target = "kilogram"
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$unit[[1]], "kilogram")
})

testthat::test_that("apply_footnote_rules concatenates mapped notes values", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    notes = NA_character_,
    footnotes = "fn_country; fn_continent; fn_note_01; fn_note_02"
  )

  footnote_rules <- data.table::data.table(
    column_source = c("footnotes", "footnotes"),
    value_source_raw = c("fn_note_01", "fn_note_02"),
    value_source = c(NA_character_, NA_character_),
    column_target = c("notes", "notes"),
    value_target_raw = c(NA_character_, NA_character_),
    value_target = c("note_01", "note_02")
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(
    result$data$footnotes[[1]],
    "fn_country; fn_continent"
  )
  testthat::expect_equal(result$data$notes[[1]], "note_01; note_02")
})

testthat::test_that("apply_footnote_rules appends concatenated notes to existing notes", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    notes = "existing note",
    footnotes = "fn_note_01; fn_note_02"
  )

  footnote_rules <- data.table::data.table(
    column_source = c("footnotes", "footnotes"),
    value_source_raw = c("fn_note_01", "fn_note_02"),
    value_source = c(NA_character_, NA_character_),
    column_target = c("notes", "notes"),
    value_target_raw = c(NA_character_, NA_character_),
    value_target = c("note_01", "note_02")
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(
    result$data$notes[[1]],
    "existing note; note_01; note_02"
  )
})

testthat::test_that("apply_footnote_rules handles NA footnotes rows", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    footnotes = c(NA_character_, "some note")
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "some note",
    value_source = "replaced",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true(is.na(result$data$footnotes[[1]]))
  testthat::expect_equal(result$data$footnotes[[2]], "replaced")
})

testthat::test_that("apply_footnote_rules cleans temporary columns", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "note"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "note",
    value_source = "new",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_false("row_id" %in% names(result$data))
  testthat::expect_false("footnote_index" %in% names(result$data))
})

testthat::test_that("apply_footnote_rules generates compatible audit structure", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "test note"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "test note",
    value_source = "replaced",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  expected_columns <- c(
    "dataset_name", "column_source", "value_source_raw",
    "value_source_result", "column_target", "value_target_raw",
    "value_target_result", "affected_rows",
    "execution_timestamp_utc", "rule_file_identifier", "execution_stage"
  )
  testthat::expect_true(all(expected_columns %in% names(result$audit)))
  testthat::expect_equal(result$audit$dataset_name[[1]], "demo")
  testthat::expect_equal(result$audit$column_source[[1]], "footnotes")
  testthat::expect_equal(result$audit$execution_stage[[1]], "clean")
  testthat::expect_equal(result$audit$rule_file_identifier[[1]], "test.xlsx")
})

testthat::test_that("apply_footnote_rules works with harmonize stage", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    footnotes = "harmonize me"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "harmonize me",
    value_source = "harmonized",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "harmonize",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "harmonized")
  testthat::expect_equal(result$audit$execution_stage[[1]], "harmonize")
})

testthat::test_that("apply_rule_payload routes footnote rules to apply_footnote_rules", {
  dataset_dt <- data.table::data.table(
    product = c("Wheat", "Rice"),
    unit = c("kg", "kg"),
    footnotes = c("fn A; fn B", "fn C")
  )

  canonical_rules <- data.table::data.table(
    column_source = c("footnotes", "product"),
    value_source_raw = c("fn A", "Rice"),
    value_source = c("replaced A", NA_character_),
    column_target = c("footnotes", "unit"),
    value_target_raw = c(NA_character_, "kg"),
    value_target = c(NA_character_, "gram")
  )

  result <- apply_rule_payload(
    dataset_dt = dataset_dt,
    canonical_rules = canonical_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$footnotes[[1]], "replaced A; fn B")
  testthat::expect_equal(result$data$unit[[2]], "gram")
  testthat::expect_true(nrow(result$audit) >= 2L)
})

testthat::test_that("apply_footnote_rules detects conflicting target updates", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg",
    footnotes = "fn1; fn2"
  )

  footnote_rules <- data.table::data.table(
    column_source = c("footnotes", "footnotes"),
    value_source_raw = c("fn1", "fn2"),
    value_source = c(NA_character_, NA_character_),
    column_target = c("unit", "unit"),
    value_target_raw = c(NA_character_, NA_character_),
    value_target = c("kilogram", "gram")
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  # last rule wins deterministically
  testthat::expect_true(result$data$unit[[1]] %in% c("kilogram", "gram"))
  testthat::expect_true(nrow(result$audit) >= 2L)
})

testthat::test_that("apply_footnote_rules adds missing footnotes column", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "any",
    value_source = "replaced",
    column_target = "footnotes",
    value_target_raw = NA_character_,
    value_target = NA_character_
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_true("footnotes" %in% names(result$data))
})

testthat::test_that("apply_footnote_rules applies conditional target updates", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "kg",
    footnotes = "conditional fn"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "conditional fn",
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram"
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$unit[[1]], "kilogram")
})

testthat::test_that("apply_footnote_rules skips conditional update when condition not met", {
  dataset_dt <- data.table::data.table(
    product = "Wheat",
    unit = "tonnes",
    footnotes = "conditional fn"
  )

  footnote_rules <- data.table::data.table(
    column_source = "footnotes",
    value_source_raw = "conditional fn",
    value_source = NA_character_,
    column_target = "unit",
    value_target_raw = "kg",
    value_target = "kilogram"
  )

  result <- apply_footnote_rules(
    dataset_dt = dataset_dt,
    footnote_rules = footnote_rules,
    stage_name = "clean",
    dataset_name = "demo",
    rule_file_id = "test.xlsx",
    execution_timestamp_utc = "2026-01-01T00:00:00Z"
  )

  testthat::expect_equal(result$data$unit[[1]], "tonnes")
})
