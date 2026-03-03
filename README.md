# FAO Digitalization Pipeline

## 1. Project Title

FAO Digitalization Pipeline

## 2. Technical Description

This repository is a script-oriented R pipeline that processes FAO source workbooks through deterministic stages:

1. General bootstrap (dependency checks, configuration construction, directory preparation)
2. Import (file discovery, read, transformation, validation)
3. Post-processing (audit, cleaning, unit standardization, harmonization)
4. Export (processed layer workbooks and unique-list outputs)

Execution is orchestrated by `scripts/run_pipeline.R`, which sources stage runners in fixed order.

## 3. Installation (renv enforced)

### Prerequisites

- R >= 4.1
- `renv`

### Setup

```r
install.packages("renv")
renv::init(bare = TRUE)
renv::install(c(
  "checkmate", "cli", "data.table", "dplyr", "fs", "here", "openxlsx",
  "progressr", "purrr", "readr", "readxl", "stringi", "stringr",
  "testthat", "withr"
))
renv::snapshot()
```

## 4. Dependency Management

- Runtime dependencies and load/check helpers are defined in `scripts/0-general_pipeline/00-dependencies.R`.
- Project setup constants and configuration constructors are defined in `scripts/0-general_pipeline/01-setup.R`.
- Shared helpers are defined in `scripts/0-general_pipeline/02-helpers.R`.
- Dependency installation and version locking are expected to be managed via `renv`.

## 5. Quick Start (deterministic example)

```r
source(here::here("scripts", "run_pipeline.R"), local = TRUE)

options(
  fao.run_pipeline.auto = FALSE,
  fao.run_general_pipeline.auto = FALSE,
  fao.run_import_pipeline.auto = FALSE,
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_export_pipeline.auto = FALSE
)

run_pipeline(
  show_view = FALSE,
  pipeline_root = here::here("scripts")
)
```

## 6. Exported API Overview

Primary API exposed via `scripts/run_pipeline.R`:

- `run_pipeline(show_view = interactive(), pipeline_root = here::here("scripts"))`

Core stage entry points used by the orchestrator:

- `run_general_pipeline(dataset_name = get_pipeline_constants()$dataset_default_name)`
- `run_import_pipeline(config)`
- `run_post_processing_pipeline_batch(raw_dt, config, dataset_name, unit_column, value_column, product_column)`
- `run_export_pipeline(config, data_objects = NULL, overwrite = TRUE, env = .GlobalEnv)`

Auto-run wrappers:

- `run_import_pipeline_auto(auto_run, env = .GlobalEnv)`
- `run_post_processing_pipeline_auto(auto_run, env = .GlobalEnv)`
- `run_export_pipeline_auto(auto_run, env = .GlobalEnv)`

Contract helpers:

- `assert_transform_result_contract(transform_result)`
- `assert_export_paths_contract(export_result)`

## 7. Architecture Overview

- `scripts/0-general_pipeline/`
  - `00-dependencies.R`: dependency registry/check/load
  - `01-setup.R`: constants and configuration
  - `02-helpers.R`: shared utility functions
  - `run_general_pipeline.R`: stage bootstrap
- `scripts/1-import_pipeline/`: file IO, reading, transforms, validation, import runner
- `scripts/2-post_processing_pipeline/`: audit, clean, standardize units, harmonize, diagnostics, post-processing runner
- `scripts/3-export_pipeline/`: processed-data and unique-list exporters, export runner
- `scripts/run_pipeline.R`: global orchestrator
- `tests/testthat/scripts/`: deterministic `testthat` suites

## 8. Engineering Standards

- Native pipe `|>`
- Snake case naming
- `<-` assignment
- Explicit `return()` in functions
- Input validation through `checkmate`
- Structured diagnostics/errors through `cli`
- Function-level roxygen documentation in script files
- Deterministic stage ordering and deterministic output contracts

## 9. Performance Notes (if benchmarks exist)

No committed benchmark artifacts are present. Current optimizations are implemented in code paths (for example, keyed `data.table` joins and minimized repeated normalization in unit-standardization functions).

## 10. Reproducibility & Determinism

- Deterministic execution order is fixed by the orchestrator.
- Centralized constants (`get_pipeline_constants()`) reduce hard-coded drift across stages.
- Data contracts are enforced with explicit assertions.
- Pipeline behavior is designed to be deterministic for identical inputs and options.

## 11. Testing & Coverage

Run all tests:

```r
source(here::here("tests", "testthat", "test_all.r"), echo = FALSE)
```

Run test directory directly:

```r
testthat::test_dir(here::here("tests", "testthat", "scripts"), reporter = "summary")
```

Coverage notes:

- Tests include layer detection, post-processing rule handling, schema contracts, compatibility aliases, and pipeline path validation.
- Formal coverage report artifacts are not committed in this repository.

## 12. CI/CD

No CI workflow configuration is currently committed under `.github/workflows`.

Recommended baseline CI pipeline:

1. Restore environment with `renv`
2. Execute `testthat` suite
3. Fail on any non-zero test result

## 13. Backward Compatibility Policy

- Preserve function signatures and return schemas for existing stage runners and wrappers.
- Maintain compatibility aliases when renaming symbols.
- When incompatible changes are unavoidable, provide wrappers and deprecation path before removal.

## 14. Contributing

- Keep changes scoped and deterministic.
- Update/add tests with every behavior or contract change.
- Reuse centralized constants from `01-setup.R` for options, script names, and object names.
- Avoid introducing implicit global-state dependencies outside orchestration entry points.

## 15. License

No license file is currently present in the repository. Add a `LICENSE` file to define usage and redistribution terms.
