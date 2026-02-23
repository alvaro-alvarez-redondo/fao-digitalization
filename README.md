# FAO Digitalization Pipeline

## 1. Short Technical Description

`faodigitalization` is an R (>= 4.1) pipeline-style repository for workbook ingestion, transformation, validation, auditing, and export. The codebase is organized as stage scripts under `R/` and exposes orchestration and audit APIs through `NAMESPACE` exports.

## 2. Installation

### 2.1 Prerequisites

- R >= 4.1
- `renv`

### 2.2 Project-local dependency bootstrap with `renv`

```r
install.packages("renv")
renv::init(bare = TRUE)
renv::install(c(
  "checkmate", "cli", "data.table", "dplyr", "fs", "here", "openxlsx",
  "progressr", "purrr", "readr", "readxl", "stringi", "stringr",
  "tibble", "tidyr", "tidyselect", "testthat", "withr"
))
```

### 2.3 Development install

```r
renv::install(".")
```

## 3. Dependency Management

Dependencies are declared in `DESCRIPTION`:

- Runtime imports: `checkmate`, `cli`, `data.table`, `dplyr`, `fs`, `here`, `openxlsx`, `progressr`, `purrr`, `readr`, `readxl`, `stringi`, `stringr`, `tibble`, `tidyr`, `tidyselect`
- Suggested test/development dependencies: `testthat`, `withr`
- R runtime: `R (>= 4.1)`

Dependency policy is repository-local isolation with `renv`. A lockfile is expected for strict version pinning in production workflows.

## 4. Quick Start Example

```r
source(here::here("R", "run_pipeline.R"), local = TRUE)

# deterministic invocation (no viewer side effects)
run_pipeline(
  show_view = FALSE,
  pipeline_root = here::here("R")
)
```

## 5. Core API Overview

Exported functions:

- `run_pipeline()`: orchestrates general, import, and export pipeline stages.
- `prepare_audit_root()`: removes a previous audit root directory when present.
- `empty_audit_findings_dt()`: returns a typed empty audit findings schema.
- `load_audit_config()`: validates audit-related configuration requirements.
- `resolve_audit_output_paths()`: builds audit workbook and mirror output paths.
- `audit_character_non_empty()`: validates character fields for non-empty values.
- `audit_numeric_string()`: validates numeric-string fields.
- `run_master_validation()`: executes configured audit validators and aggregates findings.
- `resolve_audit_columns_by_type()`: resolves validator-to-column mapping from config.
- `export_validation_audit_report()`: writes audit findings workbook.
- `mirror_raw_import_errors()`: copies raw import files related to invalid audit rows.
- `audit_data_output()`: runs end-to-end audit output flow and returns audited data.

## 6. Architecture Overview

### 6.1 Source layout

- `R/0-general_pipeline/`: dependency checks, setup/configuration, shared helpers.
- `R/1-import_pipeline/`: file discovery, reading, transformations, validation, output shaping.
- `R/3-export_pipeline/`: audit rules and export/mirroring utilities.
- `R/run_pipeline.R`: top-level orchestration entrypoint.

### 6.2 Separation of concerns

- General stage prepares dependencies/configuration and directory scaffolding.
- Import stage ingests and normalizes workbook content.
- Export stage validates consolidated records, emits audit artifacts, and writes outputs.

### 6.3 Test organization

Tests mirror source structure under `tests/testthat/r/`:

- `tests/testthat/r/0_general_pipeline/`
- `tests/testthat/r/1_import_pipeline/`
- `tests/testthat/r/3_export_pipeline/`
- root-level orchestration contract tests in `tests/testthat/r/`

Mapping details are documented in `docs/tests_reorganization_mapping.md`.

### 6.4 Validation strategy

- function-entry validation uses `checkmate`
- user-facing failures/status use `cli`
- explicit namespace usage is applied throughout stage scripts

## 7. Engineering Standards

- R version requirement: `R >= 4.1`
- Native pipe: `|>` only
- Naming: snake_case
- Validation: `checkmate` at function boundaries
- Messaging: `cli` (`cli_abort`, `cli_warn`, `cli_alert_*`)
- Documentation: roxygen2 with `DESCRIPTION`/`NAMESPACE` alignment
- Testing: deterministic `testthat` tests with isolated temporary paths and no network usage

## 8. Performance Notes

No benchmark suite or benchmark artifacts are currently committed in the repository.

## 9. Reproducibility & Determinism

- Dependency isolation policy is `renv`-based.
- A committed `renv.lock` is required for strict reproducibility across environments.
- Tests are designed to be deterministic and rely on local temporary directories.
- The repository avoids network access in tests and does not rely on unseeded randomness.

## 10. Testing & Coverage

### 10.1 Run full test tree

```r
source(here::here("tests", "testthat", "test_all.r"), local = TRUE)
```

### 10.2 Direct testthat invocation

```r
testthat::test_dir(here::here("tests", "testthat", "r"), reporter = "summary")
```

Current coverage includes helper, import, export/audit, and orchestration contracts, plus TODO placeholders for currently unscoped script-specific tests.

## 11. CI/CD Status

No CI/CD workflow configuration is currently committed (for example, no `.github/workflows/` pipeline files were detected).

## 12. Backward Compatibility Policy

- Versioning baseline is defined in `DESCRIPTION` (`Version: 0.1.0`).
- Public API compatibility is enforced through signature/default-focused tests where available.
- When a breaking change is required, use a staged deprecation lifecycle:
  1. introduce compatibility wrapper
  2. add deprecation signaling
  3. remove legacy path in a major version increment
- Return types for exported functions should remain stable unless explicitly versioned as breaking.

## 13. Contributing Guidelines

- Follow repository standards: snake_case, native pipe, explicit namespaces, checkmate validation, cli messaging.
- Keep changes deterministic and avoid network-bound tests.
- Add or update tests under the mirrored `tests/testthat/r/` tree.
- Keep roxygen2 documentation synchronized with function signatures and exports.

## 14. License

MIT License (`MIT + file LICENSE`).
