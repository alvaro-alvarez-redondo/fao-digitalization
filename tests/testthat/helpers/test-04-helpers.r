testthat::test_that("normalize_string returns ascii lowercase text", {
  normalized <- normalize_string("Árvore! DATA 2024")

  testthat::expect_identical(normalized, "arvore data 2024")
})

testthat::test_that("normalize_string preserves missing values", {
  normalized <- normalize_string(c("Árvore! DATA 2024", NA_character_))

  testthat::expect_identical(normalized, c("arvore data 2024", NA_character_))
})

testthat::test_that("normalize_filename replaces spaces with underscores", {
  normalized <- normalize_filename("Food Balance Sheet")

  testthat::expect_identical(normalized, "food_balance_sheet")
})


testthat::test_that("normalize_filename replaces missing and empty values with unknown", {
  normalized <- normalize_filename(c(
    "Food Balance Sheet",
    NA_character_,
    "***"
  ))

  testthat::expect_identical(
    normalized,
    c("food_balance_sheet", "unknown", "unknown")
  )
})

testthat::test_that("extract_yearbook returns expected token window", {
  parts <- c("fao", "yb", "2020", "2021", "foo")

  testthat::expect_identical(extract_yearbook(parts), "yb_2020_2021")
  testthat::expect_true(is.na(extract_yearbook(c("fao", "yb", "2020"))))
})

testthat::test_that("extract_product removes extension from trailing token", {
  parts <- c("a", "b", "c", "d", "e", "f", "rice", "grain.xlsx")

  testthat::expect_identical(extract_product(parts), "rice_grain")
  testthat::expect_true(is.na(extract_product(c("a", "b", "c", "d", "e", "f"))))
})

testthat::test_that("ensure_data_table and validate_export_import are type stable", {
  input_df <- data.frame(x = 1:2)

  output_dt <- ensure_data_table(input_df)
  audited_dt <- validate_export_import(input_df, "dataset")

  testthat::expect_true(data.table::is.data.table(output_dt))
  testthat::expect_true(data.table::is.data.table(audited_dt))
})

testthat::test_that("generate_export_path builds a normalized export target", {
  config <- list(
    paths = list(
      data = list(
        exports = list(
          processed = tempfile("processed_"),
          lists = tempfile("lists_")
        )
      )
    ),
    export_config = list(
      data_suffix = "_data.xlsx",
      list_suffix = "_list.xlsx"
    )
  )

  output_path <- generate_export_path(
    config = config,
    base_name = "food balance",
    type = "processed",
    use_here = FALSE
  )

  testthat::expect_true(fs::is_dir(config$paths$data$exports$processed))
  testthat::expect_match(output_path, "food_balance_data\\.xlsx$")
})


testthat::test_that("generate_export_path resolves relative folders with here", {
  config <- list(
    paths = list(
      data = list(
        exports = list(
          processed = "tmp/export_outputs",
          lists = "tmp/export_lists"
        )
      )
    ),
    export_config = list(
      data_suffix = "_data.xlsx",
      list_suffix = "_list.xlsx"
    )
  )

  output_path <- generate_export_path(
    config = config,
    base_name = "food balance",
    type = "processed"
  )

  testthat::expect_identical(
    fs::path_norm(output_path),
    fs::path_norm(here::here("tmp/export_outputs/food_balance_data.xlsx"))
  )
})

testthat::test_that("helper validators fail with cli errors on bad inputs", {
  testthat::expect_error(normalize_string(NULL), class = "rlang_error")
  testthat::expect_error(ensure_data_table("invalid"), class = "rlang_error")
  testthat::expect_error(
    generate_export_path(list(), "name"),
    class = "rlang_error"
  )
})


testthat::test_that("get_config_string validates nested config fields", {
  config <- list(
    paths = list(
      data = list(exports = list(processed = tempfile("processed_")))
    )
  )

  output_value <- get_config_string(
    config = config,
    path = c("paths", "data", "exports", "processed"),
    field_name = "config$paths$data$exports$processed"
  )

  testthat::expect_type(output_value, "character")
  testthat::expect_length(output_value, 1)
})

testthat::test_that("generate_export_path fails with explicit missing-field errors", {
  config_missing_paths <- list(
    export_config = list(data_suffix = "_data.xlsx", list_suffix = "_list.xlsx")
  )

  config_missing_suffix <- list(
    paths = list(
      data = list(
        exports = list(
          processed = tempfile("processed_"),
          lists = tempfile("lists_")
        )
      )
    ),
    export_config = list(list_suffix = "_list.xlsx")
  )

  testthat::expect_error(
    generate_export_path(config_missing_paths, "dataset", "processed"),
    "config\$paths\$data\$exports\$processed"
  )

  testthat::expect_error(
    generate_export_path(config_missing_suffix, "dataset", "processed"),
    "config\$export_config\$data_suffix"
  )
})
