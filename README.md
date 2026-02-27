# FAO Digitalization Pipeline

## 1. Project Title

FAO Digitalization Pipeline

## 2. Short Technical Description

This repository implements a script-oriented R data pipeline for ingestion, transformation, validation, post-processing (cleaning and harmonization), audit artifact generation, and export of FAO workbook data. Execution is stage-based and orchestrated through `R/run_pipeline.R`.

## 3. Installation

### 3.1 Prerequisites

- R >= 4.1
- `renv`

### 3.2 Project-local environment bootstrap with `renv`

```r
install.packages("renv")
renv::init(bare = TRUE)
renv::install(c(
  "checkmate", "cli", "data.table", "dplyr", "fs", "here", "openxlsx",
  "progressr", "purrr", "readr", "readxl", "renv", "stringi", "stringr",
  "tibble", "tidyr", "tidyselect", "testthat", "withr"
))
```

### 3.3 Development install workflow

```r
renv::activate()
# this repository is script-first; source scripts directly for development
source(here::here("R", "run_pipeline.R"), local = TRUE)
```

## 4. Dependency Management

- Runtime dependencies are centralized in `R/0-general_pipeline/00-dependencies.R` (`required_packages`, `check_dependencies()`, `load_dependencies()`).
- Dependency auditing is available via:
  - `collect_namespaced_dependencies()`
  - `audit_dependency_registry()`
- Dependency installation is designed to run via `renv::install()` under project-local environments.
- Current repository state has no `DESCRIPTION`/`NAMESPACE` package manifest and no `renv.lock`; dependency governance is script-driven.

## 5. Quick Start Example

```r
# deterministic invocation from repository root
source(here::here("R", "run_pipeline.R"), local = TRUE)

options(
  fao.run_pipeline.auto = FALSE,
  fao.run_general_pipeline.auto = FALSE,
  fao.run_import_pipeline.auto = FALSE,
  fao.run_post_processing_pipeline.auto = FALSE,
  fao.run_export_pipeline.auto = FALSE
)

run_pipeline(
  show_view = FALSE,
  pipeline_root = here::here("R")
)
```

## 6. Core API Overview

### Top-level orchestration

- `run_pipeline(show_view = interactive(), pipeline_root = here::here("R"))`: Runs all stages in order.

### General stage

- `run_general_pipeline(dataset_name = "fao_data_raw")`: Loads dependencies and config.
- `load_pipeline_config(dataset_name = "fao_data_raw", ...)`: Builds deterministic path/config structure.

### Import stage

- `run_import_pipeline(config)`: Discovers, reads, transforms, validates import files.
- `run_import_pipeline_auto(auto_run, env = .GlobalEnv)`: Conditional environment-driven auto-run.

### Post-processing stage

- `run_post_processing_pipeline_batch(raw_dt, config, unit_column, value_column, product_column)`: Executes audit, cleaning, taxonomy harmonization, unit harmonization.
- `run_post_processing_pipeline_auto(auto_run, env = .GlobalEnv)`: Conditional environment-driven auto-run.

### Export stage

- `run_export_pipeline(fao_data_raw, config, overwrite = TRUE)`: Exports processed dataset and unique-list workbook.
- `run_export_pipeline_auto(auto_run, env = .GlobalEnv)`: Conditional environment-driven auto-run.

### Audit and contracts

- `audit_data_output(dataset_dt, config)`: Produces audit findings, mirror artifacts, and parsed output.
- `run_master_validation(dataset_dt, audit_columns_by_type, selected_validations = NULL)`: Validator dispatch for audit findings.
- `assert_transform_result_contract(transform_result)`: Enforces stable import transform output schema.
- `assert_export_paths_contract(export_result)`: Enforces stable export output schema.

## 7. Architecture Overview

### Repository layout

- `R/0-general_pipeline/`: Dependency checks, setup, shared helpers, general bootstrap.
- `R/1-import_pipeline/`: File discovery, reading, transformation, validation, import output assembly.
- `R/2-post_processing_pipeline/`: Audit, cleaning, categorical harmonization, numeric unit standardization, stage orchestration.
- `R/3-export_pipeline/`: Data export and unique-list export.
- `R/run_pipeline.R`: Global stage orchestrator.
- `tests/testthat/r/`: Stage-scoped and cross-stage tests.

### Separation of concerns

- Orchestration scripts coordinate stage execution and diagnostics wiring.
- Stage scripts hold business logic and schema validation.
- Utility scripts centralize helper behaviors (rule IO, schema checks, diagnostics builders).

### Validation strategy

- Primary validation uses `checkmate` assertions/checks at function boundaries.
- Runtime messaging and fail-fast errors use `cli`.
- Contract helpers enforce stable list/data-table outputs in critical stage boundaries.

## 8. Engineering Standards

- **R version requirement**: R >= 4.1.
- **Pipe standard**: Native pipe `|>` only.
- **Naming policy**: snake_case for functions and variables.
- **Validation policy**: `checkmate` assertions/checks on external inputs and schema-sensitive internals.
- **Messaging policy**: `cli::cli_warn()`, `cli::cli_alert_info()`, `cli::cli_abort()` for user-facing diagnostics.
- **Documentation policy**: function-level roxygen2 blocks in scripts.
- **Testing policy**: deterministic `testthat` tests with explicit edge-case and contract checks.

## 9. Performance Notes

No benchmark artifacts or benchmark scripts are currently committed in the repository. Performance improvements in pipeline scripts are implemented via vectorized `data.table` joins and reducer-based stage application patterns.

## 10. Reproducibility & Determinism

- Reproducibility is designed around `renv` project-local environments.
- A `renv.lock` file is not currently present; strict version pinning is therefore not fully enforced.
- Deterministic behavior is emphasized through:
  - explicit schema contracts,
  - deterministic stage ordering,
  - no stochastic transformations in stage logic.
- No seeded randomness policy is required by current pipeline logic because no random operations are used.

## 11. Testing & Coverage

### Test execution

```r
source(here::here("tests", "testthat", "test_all.r"), echo = FALSE)
```

or:

```r
testthat::test_dir(here::here("tests", "testthat", "r"), reporter = "summary")
```

### Coverage status

- Test suites cover general, import, post-processing, export, and pipeline-runner compatibility paths.
- Formal numeric coverage reports are not committed (no `covr` artifact present).

## 12. CI/CD Status

- No CI workflow configuration is currently committed (`.github/workflows` not present).
- Recommended baseline: add a CI pipeline running `renv::restore()` and `testthat::test_dir()` on each pull request.

## 13. Backward Compatibility Policy

- Stage-runner signatures should remain stable unless a breaking change is explicitly documented.
- Contract helpers should be used to preserve expected list/data-table output structures.
- Recommended semantic versioning policy for future package-style evolution:
  - patch: non-breaking fixes,
  - minor: backward-compatible additions,
  - major: breaking changes.
- Deprecation lifecycle recommendation:
  1. introduce wrapper with warning,
  2. retain compatibility for at least one minor cycle,
  3. remove in next major release.

## 14. Contributing Guidelines

- Keep changes scoped to a single stage when possible.
- Preserve deterministic behavior and explicit validation.
- Add/adjust tests alongside code changes, including contract tests for stage boundaries.
- Use native pipe, snake_case naming, explicit `return()`, and `checkmate`/`cli` conventions.
- Avoid introducing hidden side effects in non-orchestration functions.

## 15. License

No license file is currently present in this repository. Add a `LICENSE` file to define usage and distribution terms.
