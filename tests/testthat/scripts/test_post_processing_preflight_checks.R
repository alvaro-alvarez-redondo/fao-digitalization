testthat::test_that("preflight flags invalid file naming patterns", {
  root_dir <- tempfile("fao-preflight-")
  dir.create(root_dir, recursive = TRUE)

  cleaning_dir <- file.path(root_dir, "data", "imports", "cleaning imports")
  harmonization_dir <- file.path(
    root_dir,
    "data",
    "imports",
    "harmonization imports"
  )
  dir.create(cleaning_dir, recursive = TRUE)
  dir.create(harmonization_dir, recursive = TRUE)

  file.create(file.path(cleaning_dir, "bad_cleaning_name.xlsx"))
  file.create(file.path(harmonization_dir, "bad_harmonization_name.xlsx"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = cleaning_dir,
          harmonization = harmonization_dir
        )
      )
    )
  )

  withr::with_dir(root_dir, {
    result <- collect_post_processing_preflight(
      config = config,
      dataset_columns = c("unit", "value", "product")
    )

    testthat::expect_false(result$checks$cleaning_pattern_ok)
    testthat::expect_false(result$checks$harmonization_pattern_ok)
    testthat::expect_true(any(grepl(
      "cleaning stage",
      result$issues,
      fixed = TRUE
    )))
    testthat::expect_true(any(grepl(
      "harmonization stage",
      result$issues,
      fixed = TRUE
    )))
  })
})

testthat::test_that("preflight detects column convention mismatch", {
  root_dir <- tempfile("fao-preflight-")
  dir.create(root_dir, recursive = TRUE)

  cleaning_dir <- file.path(root_dir, "data", "imports", "cleaning imports")
  harmonization_dir <- file.path(
    root_dir,
    "data",
    "imports",
    "harmonization imports"
  )
  template_dir <- file.path(root_dir, "data", "exports", "templates")
  dir.create(cleaning_dir, recursive = TRUE)
  dir.create(harmonization_dir, recursive = TRUE)
  dir.create(template_dir, recursive = TRUE)

  file.create(file.path(cleaning_dir, "cleaning_rules.xlsx"))
  file.create(file.path(harmonization_dir, "harmonization_rules.xlsx"))
  file.create(file.path(template_dir, "numeric_harmonization_template.xlsx"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = cleaning_dir,
          harmonization = harmonization_dir
        )
      )
    )
  )

  withr::with_dir(root_dir, {
    result <- collect_post_processing_preflight(
      config = config,
      dataset_columns = c("unit", "value", "item")
    )

    testthat::expect_false(result$passed)
    testthat::expect_true(any(grepl(
      "required columns missing for this run",
      result$issues,
      fixed = TRUE
    )))
  })
})

testthat::test_that("assert_post_processing_preflight aborts with stage details", {
  bad_result <- list(
    passed = FALSE,
    issues = c(
      "[cleaning stage] missing file",
      "[numeric harmonization stage] missing template"
    ),
    checks = list()
  )

  testthat::expect_error(
    assert_post_processing_preflight(bad_result),
    regexp = "post-processing preflight checks failed"
  )
})


testthat::test_that("preflight detects auto-run naming mismatch", {
  root_dir <- tempfile("fao-preflight-")
  dir.create(root_dir, recursive = TRUE)

  cleaning_dir <- file.path(root_dir, "data", "imports", "cleaning imports")
  harmonization_dir <- file.path(
    root_dir,
    "data",
    "imports",
    "harmonization imports"
  )
  template_dir <- file.path(root_dir, "data", "exports", "templates")
  dir.create(cleaning_dir, recursive = TRUE)
  dir.create(harmonization_dir, recursive = TRUE)
  dir.create(template_dir, recursive = TRUE)

  file.create(file.path(cleaning_dir, "cleaning_rules.xlsx"))
  file.create(file.path(harmonization_dir, "harmonization_rules.xlsx"))
  file.create(file.path(template_dir, "numeric_harmonization_template.xlsx"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = cleaning_dir,
          harmonization = harmonization_dir
        )
      )
    )
  )

  withr::with_dir(root_dir, {
    result <- collect_post_processing_preflight(
      config = config,
      dataset_columns = c("unit", "value", "product"),
      expected_columns = c("unit_name", "quantity", "item")
    )

    testthat::expect_false(result$passed)
    testthat::expect_true(any(grepl(
      "auto-run expects",
      result$issues,
      fixed = TRUE
    )))
  })
})
