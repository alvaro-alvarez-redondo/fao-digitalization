options(
  fao.run_post_processing_pipeline.auto = FALSE
)

source(
  here::here(
    "scripts",
    "2-post_processing_pipeline",
    "21-post_processing_utilities.R"
  ),
  echo = FALSE
)

testthat::test_that("generate_post_processing_rule_templates writes clean and harmonize templates", {
  root_dir <- tempfile("fao-template-generation-")
  dir.create(root_dir, recursive = TRUE)

  audit_root_dir <- file.path(root_dir, "data", "2-post_processing")

  config <- list(
    paths = list(
      data = list(
        audit = list(
          audit_root_dir = audit_root_dir
        )
      )
    )
  )

  template_paths <- generate_post_processing_rule_templates(
    config = config,
    overwrite = TRUE
  )

  testthat::expect_setequal(names(template_paths), c("clean", "harmonize"))
  testthat::expect_true(all(file.exists(unname(template_paths))))

  testthat::expect_match(
    basename(template_paths[["clean"]]),
    "^clean_clean_harmonize_template\\.xlsx$"
  )
  testthat::expect_match(
    basename(template_paths[["harmonize"]]),
    "^harmonize_clean_harmonize_template\\.xlsx$"
  )
})
