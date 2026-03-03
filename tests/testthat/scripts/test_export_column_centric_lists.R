options(
  fao.run_export_pipeline.auto = FALSE
)

source(here::here("scripts", "0-general_pipeline", "02-helpers.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "30-export_data.R"), echo = FALSE)
source(here::here("scripts", "3-export_pipeline", "31-export_lists.R"), echo = FALSE)

testthat::test_that("collect_union_columns returns deterministic union", {
  layer_by_sheet <- list(
    raw = data.table::data.table(country = c("a", "b"), year = c("2020", "2021")),
    clean = data.table::data.table(country = c("a", "c")),
    standardize = data.table::data.table(unit = c("kg", "t")),
    harmonize = data.table::data.table(country = c("a"), value = c("1"))
  )

  union_columns <- collect_union_columns(layer_by_sheet)

  testthat::expect_identical(union_columns, sort(c("country", "year", "unit", "value")))
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
  testthat::expect_true(length(cache$standardize$country) == 0)
  testthat::expect_setequal(cache$raw$country, c("a", "b"))
})
