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

run tests:

```r
testthat::test_dir(here::here("tests", "testthat"))
```

## engineering notes

- the codebase uses native r pipe `|>` and avoids `%>%`.
- source code uses `cli` for user-facing errors and status output.
- tests include happy-path and edge/error coverage for core helpers.
- no network access is required for test execution.


## orchestration artifacts

- repository-wide integration report: `docs/repository_integration_report.md`
- the report captures audit summary, risk matrix, compatibility posture, and migration notes.
