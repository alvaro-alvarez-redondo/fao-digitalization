testthat::test_that("preflight flags invalid file naming patterns", {
  root_dir <- tempfile("whep-preflight-")
  dir.create(root_dir, recursive = TRUE)

  cleaning_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  harmonization_dir <- file.path(
    root_dir,
    "data",
    "1-import",
    "13-harmonize_imports"
  )
  audit_root_dir <- file.path(root_dir, "data", "2-post_processing")

  dir.create(cleaning_dir, recursive = TRUE)
  dir.create(harmonization_dir, recursive = TRUE)
  dir.create(file.path(audit_root_dir, "templates"), recursive = TRUE)
  dir.create(
    file.path(audit_root_dir, "post_processing_diagnostics"),
    recursive = TRUE
  )

  file.create(file.path(cleaning_dir, "bad_clean_name.xlsx"))
  file.create(file.path(harmonization_dir, "bad_harmonize_name.xlsx"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = cleaning_dir,
          harmonization = harmonization_dir
        ),
        audit = list(audit_root_dir = audit_root_dir)
      )
    )
  )

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
  root_dir <- tempfile("whep-preflight-")
  dir.create(root_dir, recursive = TRUE)

  cleaning_dir <- file.path(root_dir, "data", "1-import", "11-clean_imports")
  harmonization_dir <- file.path(
    root_dir,
    "data",
    "1-import",
    "13-harmonize_imports"
  )
  audit_root_dir <- file.path(root_dir, "data", "2-post_processing")

  dir.create(cleaning_dir, recursive = TRUE)
  dir.create(harmonization_dir, recursive = TRUE)
  dir.create(file.path(audit_root_dir, "templates"), recursive = TRUE)
  dir.create(
    file.path(audit_root_dir, "post_processing_diagnostics"),
    recursive = TRUE
  )

  file.create(file.path(cleaning_dir, "clean_rules.xlsx"))
  file.create(file.path(harmonization_dir, "harmonize_rules.xlsx"))

  config <- list(
    paths = list(
      data = list(
        imports = list(
          cleaning = cleaning_dir,
          harmonization = harmonization_dir
        ),
        audit = list(audit_root_dir = audit_root_dir)
      )
    )
  )

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
    regexp = "post-processing preflight checks failed"
  )
})
