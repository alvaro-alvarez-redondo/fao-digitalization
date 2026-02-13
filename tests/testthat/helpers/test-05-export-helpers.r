source(here::here("R/3-export_pipeline/32-export_lists.R"), echo = FALSE)

testthat::test_that("normalize_sheet_name is resilient to missing and empty labels", {
  normalized <- normalize_sheet_name(c(NA_character_, "***", "country"))

  testthat::expect_identical(normalized, c("unknown", "unknown", "country"))
})
