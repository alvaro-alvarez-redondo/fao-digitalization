# test runner entry point
# sources the shared helper, then runs all test directories
source(here::here("tests", "test_helper.R"), echo = FALSE)

# run tests in the new structure (mirroring scripts/ directory)
test_dirs <- c(
  here::here("tests", "0-general_pipeline"),
  here::here("tests", "1-import_pipeline"),
  here::here("tests", "2-post_processing_pipeline"),
  here::here("tests", "3-export_pipeline")
)

for (test_dir in test_dirs) {
  if (dir.exists(test_dir) && length(list.files(test_dir, pattern = "^test-")) > 0) {
    testthat::test_dir(test_dir, reporter = "summary")
  }
}

# also run legacy tests in tests/testthat/scripts/ if present
legacy_dir <- here::here("tests", "testthat", "scripts")
if (dir.exists(legacy_dir) && length(list.files(legacy_dir, pattern = "^test_")) > 0) {
  testthat::test_dir(legacy_dir, reporter = "summary")
}
