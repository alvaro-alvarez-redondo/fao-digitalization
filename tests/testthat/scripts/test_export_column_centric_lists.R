options(
  fao.run_export_pipeline.auto = FALSE
)

source(
  here::here("scripts", "0-general_pipeline", "02-helpers.R"),
  echo = FALSE
)
source(
  here::here("scripts", "3-export_pipeline", "30-export_data.R"),
  echo = FALSE
)
source(
  here::here("scripts", "3-export_pipeline", "31-export_lists.R"),
  echo = FALSE
)

testthat::test_that("collect_union_columns returns deterministic union", {
  layer_by_sheet <- list(
    raw = data.table::data.table(
      country = c("a", "b"),
      year = c("2020", "2021")
    ),
    clean = data.table::data.table(country = c("a", "c")),
    standardize = data.table::data.table(unit = c("kg", "t")),
    harmonize = data.table::data.table(country = c("a"), value = c("1"))
  )

  union_columns <- collect_union_columns(layer_by_sheet)

  testthat::expect_identical(
    union_columns,
    sort(c("country", "year", "unit", "value"))
  )
})

testthat::test_that("build_column_unique_cache returns empty vectors for missing columns", {
  layer_by_sheet <- list(
    raw = data.table::data.table(country = c("a", "b")),
    clean = data.table::data.table(),
    standardize = data.table::data.table(),
    harmonize = data.table::data.table(country = c("a", "c"))
  )

  union_columns <- collect_union_columns(layer_by_sheet)
  cache <- build_column_unique_cache(layer_by_sheet, union_columns)

  testthat::expect_true(length(cache$clean$country) == 0)
  testthat::expect_setequal(cache$raw$country, c("a", "b"))
})

testthat::test_that("export_lists excludes standardized sheet and value/year workbooks", {
  lists_dir <- tempfile("lists-export-")

  config <- list(
    paths = list(
      data = list(
        exports = list(
          lists = lists_dir
        )
      )
    )
  )

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
    demo_normalized = data.table::data.table(
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

  workbook_sheets <- openxlsx::getSheetNames(output_paths[["country"]])
  testthat::expect_false("standardize" %in% workbook_sheets)
})

testthat::test_that("write_column_lists_workbook writes raw_clean_harmonize for all-equal tables", {
  lists_dir <- tempfile("lists-equal-")

  config <- list(
    paths = list(
      data = list(
        exports = list(
          lists = lists_dir
        )
      )
    )
  )

  unique_cache <- list(
    raw = list(country = c("a", "b")),
    clean = list(country = c("a", "b")),
    harmonize = list(country = c("a", "b"))
  )

  workbook_path <- write_column_lists_workbook(
    column_name = "country",
    unique_cache = unique_cache,
    config = config,
    overwrite = TRUE
  )

  workbook_sheets <- openxlsx::getSheetNames(workbook_path)

  testthat::expect_setequal(workbook_sheets, c("raw_clean_harmonize"))
  testthat::expect_false("raw" %in% workbook_sheets)
  testthat::expect_false("clean" %in% workbook_sheets)
  testthat::expect_false("harmonize" %in% workbook_sheets)
  testthat::expect_false("clean_harmonize" %in% workbook_sheets)
})

testthat::test_that("write_column_lists_workbook writes raw + clean_harmonize when clean/harmonize equal", {
  lists_dir <- tempfile("lists-different-")

  config <- list(
    paths = list(
      data = list(
        exports = list(
          lists = lists_dir
        )
      )
    )
  )

  unique_cache <- list(
    raw = list(country = c("a", "b")),
    clean = list(country = c("c", "d")),
    harmonize = list(country = c("c", "d"))
  )

  workbook_path <- write_column_lists_workbook(
    column_name = "country",
    unique_cache = unique_cache,
    config = config,
    overwrite = TRUE
  )

  workbook_sheets <- openxlsx::getSheetNames(workbook_path)

  testthat::expect_setequal(workbook_sheets, c("raw", "clean_harmonize"))
  testthat::expect_false("clean" %in% workbook_sheets)
  testthat::expect_false("harmonize" %in% workbook_sheets)
})

testthat::test_that("write_column_lists_workbook writes raw, clean, harmonize when all differ", {
  lists_dir <- tempfile("lists-all-different-")

  config <- list(
    paths = list(
      data = list(
        exports = list(
          lists = lists_dir
        )
      )
    )
  )

  unique_cache <- list(
    raw = list(country = c("a", "b")),
    clean = list(country = c("c", "d")),
    harmonize = list(country = c("e", "f"))
  )

  workbook_path <- write_column_lists_workbook(
    column_name = "country",
    unique_cache = unique_cache,
    config = config,
    overwrite = TRUE
  )

  workbook_sheets <- openxlsx::getSheetNames(workbook_path)

  testthat::expect_setequal(workbook_sheets, c("raw", "clean", "harmonize"))
  testthat::expect_false("raw_clean_harmonize" %in% workbook_sheets)
})

testthat::test_that("clean/harmonize comparison ignores row and column order differences", {
  clean_dt <- data.table::data.table(
    value = c("x", "y", "z"),
    code = c("1", "2", "3")
  )
  harmonize_dt <- data.table::data.table(
    code = c("3", "1", "2"),
    value = c("z", "x", "y")
  )

  is_identical <- are_clean_harmonize_tables_identical(
    clean_values_dt = clean_dt,
    harmonize_values_dt = harmonize_dt
  )

  testthat::expect_true(is_identical)
})

testthat::test_that("normalize_for_comparison drops year column", {
  input_dt <- data.table::data.table(
    year = c("2020", "2021"),
    value = c("x", "y")
  )

  normalized_dt <- normalize_for_comparison(input_dt)

  testthat::expect_false("year" %in% names(normalized_dt))
  testthat::expect_true("value" %in% names(normalized_dt))
})
