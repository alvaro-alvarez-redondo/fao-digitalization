# 1. Project Title

fao-digitalization

## 2. Short Technical Description

This repository implements a staged R data pipeline for FAO workbook ingestion, normalization, validation, auditing, and export.

The orchestration flow is script-based and currently centered on `run_pipeline()` in `R/run_pipeline.R`, which sources stage runner scripts and executes:

1. `run_general_pipeline()`
2. `run_import_pipeline(config)`
3. `run_export_pipeline(fao_data_raw, config, overwrite = TRUE)`

## 3. Installation

This repository does not include `DESCRIPTION`, `NAMESPACE`, or a `renv.lock` file, so installation is currently repository-source based.

### 3.1 Recommended environment bootstrap (renv-first)

```r
# from repository root
install.packages("renv")
renv::init(bare = TRUE)
```

Then install required runtime dependencies listed in `R/0-general_pipeline/00-dependencies.R`:

```r
source(here::here("R/0-general_pipeline/00-dependencies.R"), echo = FALSE)
renv::install(required_packages)
```

### 3.2 Development setup

```r
# from repository root
source(here::here("R/0-general_pipeline/00-dependencies.R"), echo = FALSE)
load_dependencies(required_packages)
```

Do not rely on global/system library state without `renv`; dependency resolution is intended to be project-scoped.

## 4. Dependency Management

Dependency governance is implemented in `R/0-general_pipeline/00-dependencies.R`:

- `required_packages` defines the runtime dependency registry.
- `check_dependencies(packages)` checks availability and installs missing packages via `renv::install()`.
- `load_dependencies(packages)` attaches required packages.

Current state:

- `renv` is used as an installer backend.
- `renv.lock` is not present, so version pinning is incomplete.
- `DESCRIPTION`/`NAMESPACE` are not present, so Imports/Suggests separation is not yet formalized.

## 5. Quick Start Example

Minimal deterministic example without file I/O side effects:

```r
source(here::here("R/0-general_pipeline/02-helpers.R"), echo = FALSE)

normalized <- normalize_filename("My Dataset 2024.xlsx")
print(normalized)
# [1] "my_dataset_2024"
```

Pipeline execution entrypoint:

```r
source(here::here("R/run_pipeline.R"), echo = FALSE)
run_pipeline(show_view = FALSE)
```

## 6. Core API Overview

### 6.1 Orchestration and stage runners

- `run_pipeline(show_view = interactive(), pipeline_root = here::here("R"))`  
  Source stage runner scripts and execute general/import/export stages in order.
- `run_general_pipeline(dataset_name = "fao_data_raw")`  
  Load dependencies, build configuration, and create required directories.
- `run_import_pipeline(config)`  
  Discover files, read sheets, transform data, validate records, and consolidate output.
- `run_export_pipeline(fao_data_raw, config, overwrite = TRUE)`  
  Run audit export, processed dataset export, and unique-list export.

### 6.2 Exported audit helpers (`R/3-export_pipeline/30-data_audit.R`)

- `prepare_audit_root()`
- `empty_audit_findings_dt()`
- `load_audit_config()`
- `resolve_audit_output_paths()`
- `audit_character_non_empty()`
- `audit_numeric_string()`
- `run_master_validation()`
- `resolve_audit_columns_by_type()`
- `export_validation_audit_report()`
- `mirror_raw_import_errors()`
- `audit_data_output()`

### 6.3 Backward-compatibility wrappers

Deprecated wrappers are available for source-time legacy behavior:

- `legacy_source_run_pipeline()`
- `legacy_source_run_general_pipeline()`
- `legacy_source_run_import_pipeline()`
- `legacy_source_run_export_pipeline()`

## 7. Architecture Overview

Repository structure:

- `R/0-general_pipeline`  
  Dependency checks, configuration loading, helpers, and general bootstrap runner.
- `R/1-import_pipeline`  
  File discovery, sheet reading, transformation, validation, and consolidated output shaping.
- `R/3-export_pipeline`  
  Audit generation, processed dataset export, and unique-list workbook export.
- `tests/testthat`  
  Deterministic tests grouped by:
  - `data_integrity` (3 files)
  - `transformations` (2 files)
  - `edge_cases` (1 file)
  - `helpers` (6 files)
- `tests/testthat` orchestrator scripts  
  `test_all.r` composes deterministic suites by concern area and serves as the test execution index.

Separation of concerns:

- Stage runners orchestrate stage-specific script sourcing and execution.
- Import path handles shape normalization and schema consolidation before export.
- Export path handles audit/report generation and output publication.

Validation strategy:

- Function entry validation uses `checkmate` checks/assertions.
- User-facing errors/warnings/info messages use `cli`.
- Schema and behavior constraints are asserted in testthat suites.

## 8. Engineering Standards

Project engineering standards currently enforced in code and tests:

- R version target: `>= 4.1`.
- Native pipe: `|>`.
- Naming convention: `snake_case` function/object names.
- Validation policy: `checkmate` validation at function boundaries.
- Messaging policy: `cli` for user-facing errors/warnings/info.
- Documentation standard: roxygen2 blocks in script functions.
- Testing policy: deterministic `testthat` tests, including happy-path, edge-case, and error-path checks.

## 9. Performance Notes (if applicable)

No committed benchmark harness is currently present in this repository tree.

Performance guidance:

- treat workbook read/transform stages in `R/1-import_pipeline` as likely hotspots due to row-wise filtering and reshaping.
- add environment-local benchmark scripts only when performance tuning is explicitly requested, then exclude generated result artifacts from version control.

## 10. Reproducibility & Determinism

Current reproducibility posture:

- Path management uses `here::here()`.
- Tests are organized for deterministic execution through `tests/testthat/test_all.r` and scope-specific suites.
- No non-seeded randomness should be introduced in tests; benchmark scripts may use random sampling and are not test gates.

Current gaps:

- `renv.lock` is not present, so full version pinning is incomplete.
- Without lockfile pinning, cross-machine dependency reproducibility is not guaranteed.

## 11. Testing & Coverage

Test entrypoints:

```r
source(here::here("tests/testthat.R"), echo = FALSE)
```

The test suite executes grouped directories via `tests/testthat/test_all.r`:

- data integrity contracts
- transformation behavior
- edge cases
- helper and runner behavior

Coverage characteristics (qualitative):

- orchestration behavior tests for `run_pipeline` and stage runner wrappers.
- import/output contract tests including schema ordering and empty-output behavior.
- dependency helper tests for check/load behavior.

No automated percentage coverage report is currently tracked in repository artifacts.

## 12. CI/CD Status

No CI/CD workflow configuration was detected in `.github/workflows`.

Status:

- automated CI pipeline: not configured in repository.
- automated coverage reporting: not configured in repository.

## 13. Backward Compatibility Policy

Compatibility model applied in current codebase:

- preserve runner function signatures and default arguments unless explicitly authorized.
- preserve return structure contracts for runner outputs.
- provide deprecated legacy wrappers when behavior shifts from implicit source-time side effects to explicit function execution.

Deprecation lifecycle (maintained in this README until dedicated docs are added):

1. Introduce wrapper + warning.
2. Keep wrapper in next release with migration guidance.
3. Remove wrapper in subsequent release.

Versioning note:

- semantic versioning policy is recommended but not currently formalized in package metadata because `DESCRIPTION` is absent.

## 14. Contributing Guidelines

For contributions in this repository:

1. Keep function and object names in `snake_case`.
2. Use native pipe `|>` only.
3. Add `checkmate` validation at function boundaries.
4. Use `cli` for user-facing errors and warnings.
5. Keep file paths project-rooted via `here::here()`.
6. Add or update deterministic `testthat` tests for changed behavior.
7. Update this README section (and add dedicated docs when introduced) when architectural contracts are changed.

## 15. License

No license file was detected in the repository root.

Before external distribution, add an explicit project license file (for example `LICENSE` or `LICENSE.md`) and align repository metadata accordingly.
