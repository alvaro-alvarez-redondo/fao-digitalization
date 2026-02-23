# fao-digitalization

an r pipeline package-like repository for importing, transforming, validating, and exporting fao digitalization workbooks.

## repository structure

- `R/0-general_pipeline/`: dependency checks, setup, and shared helpers.
- `R/1-import_pipeline/`: file io, read, transform, validation, and output for imported workbook data.
- `R/3-export_pipeline/`: data audit and export helpers for output artifacts.
- `R/run_pipeline.R`: top-level pipeline orchestrator.
- `tests/testthat/`: deterministic unit tests organized by area.

## runtime requirements

- r >= 4.1
- `renv` for dependency isolation
- packages referenced with explicit namespaces in source files

## execution

run the full pipeline:

```r
source(here::here("R", "run_pipeline.R"), local = TRUE)
run_pipeline(show_view = FALSE, pipeline_root = here::here("R"))
```

run tests:

```r
testthat::test_dir(here::here("tests", "testthat"))
```

## engineering notes

- the codebase uses native r pipe `|>` and avoids `%>%`.
- source code uses `cli` for user-facing errors and status output.
- tests include happy-path and edge/error coverage for core helpers.
- no network access is required for test execution.

