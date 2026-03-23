# tests/3-export_pipeline/test-export-lists.R
# unit tests for scripts/3-export_pipeline/31-export_lists.R

source(here::here("tests", "test_helper.R"), echo = FALSE)
source(
  here::here("scripts", "3-export_pipeline", "30-export_data.R"),
  echo = FALSE
)
source(
  here::here("scripts", "3-export_pipeline", "31-export_lists.R"),
  echo = FALSE
)


# --- get_lists_sheet_order ---------------------------------------------------

testthat::test_that("get_lists_sheet_order returns fixed order", {
  result <- get_lists_sheet_order()

  testthat::expect_identical(result, c("raw", "clean", "harmonize"))
})


# --- build_layer_tables_by_sheet ---------------------------------------------

testthat::test_that("build_layer_tables_by_sheet enforces fixed sheet keys", {
  layer_tables <- list(
    whep_data_raw = data.frame(country = c("a", "b")),
    whep_data_harmonized = data.frame(country = c("a", "c"))
  )

  result <- build_layer_tables_by_sheet(layer_tables)

  testthat::expect_identical(names(result), c("raw", "clean", "harmonize"))
  testthat::expect_true(nrow(result$clean) == 0)
})

testthat::test_that("build_layer_tables_by_sheet handles empty layer list", {
  result <- build_layer_tables_by_sheet(list())

  testthat::expect_identical(names(result), c("raw", "clean", "harmonize"))
  testthat::expect_true(all(vapply(result, nrow, integer(1)) == 0L))
})


# --- collect_union_columns ---------------------------------------------------

testthat::test_that("collect_union_columns returns sorted unique union", {
  layer_by_sheet <- list(
    raw = data.table::data.table(country = c("a"), year = c("2020")),
    clean = data.table::data.table(country = c("a")),
    harmonize = data.table::data.table(country = c("a"), value = c("1"))
  )

  result <- collect_union_columns(layer_by_sheet)

  testthat::expect_identical(result, sort(c("country", "year", "value")))
})

testthat::test_that("collect_union_columns handles empty tables", {
  layer_by_sheet <- list(
    raw = data.table::data.table(),
    clean = data.table::data.table(),
    harmonize = data.table::data.table()
  )

  result <- collect_union_columns(layer_by_sheet)

  testthat::expect_equal(length(result), 0L)
})


# --- build_column_unique_cache -----------------------------------------------

testthat::test_that("build_column_unique_cache returns empty vectors for missing columns", {
  layer_by_sheet <- list(
    raw = data.table::data.table(country = c("a", "b")),
    clean = data.table::data.table(),
    harmonize = data.table::data.table(country = c("a", "c"))
  )

  union_columns <- collect_union_columns(layer_by_sheet)
  cache <- build_column_unique_cache(layer_by_sheet, union_columns)

  testthat::expect_true(length(cache$clean$country) == 0)
  testthat::expect_setequal(cache$raw$country, c("a", "b"))
  testthat::expect_setequal(cache$harmonize$country, c("a", "c"))
})


# --- normalize_for_comparison ------------------------------------------------

testthat::test_that("normalize_for_comparison drops year column", {
  input_dt <- data.table::data.table(
    year = c("2020", "2021"),
    value = c("x", "y")
  )

  result <- normalize_for_comparison(input_dt)

  testthat::expect_false("year" %in% names(result))
  testthat::expect_true("value" %in% names(result))
})

testthat::test_that("normalize_for_comparison sorts rows and columns", {
  input_dt <- data.table::data.table(
    b = c("2", "1"),
    a = c("y", "x")
  )

  result <- normalize_for_comparison(input_dt)

  testthat::expect_identical(names(result), c("a", "b"))
  testthat::expect_equal(result$a, c("x", "y"))
})


# --- are_list_tables_identical -----------------------------------------------

testthat::test_that("clean/harmonize comparison ignores row and column order", {
  clean_dt <- data.table::data.table(
    value = c("x", "y", "z"),
    code = c("1", "2", "3")
  )
  harmonize_dt <- data.table::data.table(
    code = c("3", "1", "2"),
    value = c("z", "x", "y")
  )

  result <- are_list_tables_identical(clean_dt, harmonize_dt)

  testthat::expect_true(result)
})

testthat::test_that("are_list_tables_identical detects different values", {
  dt1 <- data.table::data.table(value = c("x", "y"))
  dt2 <- data.table::data.table(value = c("x", "z"))

  result <- are_list_tables_identical(dt1, dt2)

  testthat::expect_false(result)
})


# --- resolve_list_sheet_payloads ---------------------------------------------

testthat::test_that("all-equal layers produce single combined sheet", {
  dt <- data.table::data.table(value = c("a", "b"))

  result <- resolve_list_sheet_payloads(dt, dt, dt)

  testthat::expect_identical(names(result), "raw_clean_harmonize")
})

testthat::test_that("raw different, clean=harmonize produce raw + clean_harmonize", {
  raw <- data.table::data.table(value = c("a"))
  clean <- data.table::data.table(value = c("b"))
  harmonize <- data.table::data.table(value = c("b"))

  result <- resolve_list_sheet_payloads(raw, clean, harmonize)

  testthat::expect_setequal(names(result), c("raw", "clean_harmonize"))
})

testthat::test_that("all different produce raw + clean + harmonize", {
  raw <- data.table::data.table(value = c("a"))
  clean <- data.table::data.table(value = c("b"))
  harmonize <- data.table::data.table(value = c("c"))

  result <- resolve_list_sheet_payloads(raw, clean, harmonize)

  testthat::expect_setequal(names(result), c("raw", "clean", "harmonize"))
})


# --- write_column_lists_workbook ---------------------------------------------

testthat::test_that("write_column_lists_workbook writes all-equal as raw_clean_harmonize", {
  config <- build_test_config()

  unique_cache <- list(
    raw = list(country = c("a", "b")),
    clean = list(country = c("a", "b")),
    harmonize = list(country = c("a", "b"))
  )

  path <- write_column_lists_workbook(
    column_name = "country",
    unique_cache = unique_cache,
    config = config,
    overwrite = TRUE
  )

  sheets <- readxl::excel_sheets(path)
  testthat::expect_setequal(sheets, "raw_clean_harmonize")
})

testthat::test_that("write_column_lists_workbook writes raw + clean_harmonize", {
  config <- build_test_config()

  unique_cache <- list(
    raw = list(country = c("a", "b")),
    clean = list(country = c("c", "d")),
    harmonize = list(country = c("c", "d"))
  )

  path <- write_column_lists_workbook(
    column_name = "country",
    unique_cache = unique_cache,
    config = config,
    overwrite = TRUE
  )

  sheets <- readxl::excel_sheets(path)
  testthat::expect_setequal(sheets, c("raw", "clean_harmonize"))
})

testthat::test_that("write_column_lists_workbook writes all three sheets", {
  config <- build_test_config()

  unique_cache <- list(
    raw = list(country = c("a", "b")),
    clean = list(country = c("c", "d")),
    harmonize = list(country = c("e", "f"))
  )

  path <- write_column_lists_workbook(
    column_name = "country",
    unique_cache = unique_cache,
    config = config,
    overwrite = TRUE
  )

  sheets <- readxl::excel_sheets(path)
  testthat::expect_setequal(sheets, c("raw", "clean", "harmonize"))
})


# --- export_lists ------------------------------------------------------------

testthat::test_that("export_lists excludes value/year columns", {
  config <- build_test_config()

  data_objects <- list(
    demo_raw = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2"),
      year = c("2020", "2021")
    ),
    demo_cleaned = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2"),
      year = c("2020", "2021")
    ),
    demo_harmonized = data.table::data.table(
      country = c("a", "b"),
      value = c("1", "2"),
      year = c("2020", "2021")
    )
  )

  output_paths <- export_lists(
    config = config,
    data_objects = data_objects,
    overwrite = TRUE,
    env = new.env(parent = emptyenv())
  )

  testthat::expect_true("country" %in% names(output_paths))
  testthat::expect_false("value" %in% names(output_paths))
  testthat::expect_false("year" %in% names(output_paths))
})
