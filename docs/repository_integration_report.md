# Repository Audit Summary

## Scope and method
- Scope: repository structure, all R scripts under `R/`, and test scripts under `tests/testthat`.
- Method: static analysis only (no runtime mutation of pipeline code).
- Execution note: R runtime was not available in this environment, so test execution could not be performed.

## Repository Structure Overview
- Root orchestration entrypoints:
  - `fao-digitalization.R` (sources `R/run_pipeline.R`).
  - `R/run_pipeline.R` (top-level pipeline orchestrator).
- Stage modules under `R/`:
  - `0-general_pipeline` (dependencies, setup, helpers, stage runner).
  - `1-import_pipeline` (file discovery, reading, transform, validation, output, stage runner).
  - `2-clean_harmonize_pipeline` (cleaning/harmonization logic + auto-run wrapper).
  - `3-export_pipeline` (audit + export logic + stage runner).
- Tests:
  - `tests/testthat/r/**` granular stage-level tests.
  - `tests/testthat/test_all.r` aggregate runner.
  - `tests/testthat.R` root test bootstrap.

## Violations & Risk Matrix

| ID | Area | Severity | Finding | Evidence |
|---|---|---|---|---|
| V1 | Dependency/repository metadata | Major | `README.md` documents `DESCRIPTION`, `NAMESPACE`, and lockfile expectations, but these files are absent from repository root. | `README.md` dependency section and orchestration notes references package-style metadata and lockfile assumptions. |
| V2 | Testing/backward compatibility | Major | `run_pipeline` now sources 4 stage runners, but existing unit test still asserts only 3 sourced scripts. | `R/run_pipeline.R` includes clean-harmonize stage; `tests/testthat/r/test_run_pipeline.r` expects only general/import/export. |
| V3 | Architecture/hidden dependency | Critical | `run_clean_harmonize_pipeline_auto()` resolves environment objects into `*_value` variables but calls batch runner with different symbols (`fao_data_raw`, `config`) that are not the resolved locals. | `R/2-clean_harmonize_pipeline/run_clean_harmonize_pipeline.R` lines 44-49. |
| V4 | Documentation/roxygen completeness | Minor | `coerce_numeric_safe()` lacks roxygen block while neighboring helpers are documented. | `R/0-general_pipeline/02-helpers.R` function appears without roxygen preamble. |
| V5 | Validation convention | Minor | Multiple functions return implicit last-expression values instead of explicit `return()`, violating stated strict explicit-return convention. | Several helpers in import/validation modules omit explicit `return()`. |
| V6 | Testing completeness | Major | Placeholder TODO tests are present and skipped for setup and file IO modules. | `tests/testthat/r/0_general_pipeline/test_01_setup_todo.r`; `tests/testthat/r/1_import_pipeline/test_10_file_io_todo.r`. |

## Architectural Risk Assessment

- **Coupling risk: Medium-High**
  - Pipeline stages rely on sourcing scripts with side effects and global environment assignment patterns.
  - Auto-run behavior controlled by options can trigger stage execution on source, increasing accidental coupling.

- **Cohesion risk: Medium**
  - Functional decomposition is generally strong inside stage files.
  - However, orchestration and global assignment behavior in runners mixes control flow and environment management concerns.

- **Hidden dependency risk: High**
  - Clean-harmonize auto wrapper has a variable-resolution mismatch that can fail depending on calling environment.

- **Testing risk: Medium-High**
  - Good breadth of tests exists, but critical setup/file IO modules retain TODO skips.
  - Runtime verification was not possible in this environment.

- **Performance risk: Low-Medium**
  - Heavy use of `data.table` and vectorized operations is present.
  - No immediate anti-pattern hotspots detected from static read-only scan.

## Rewrite Necessity Classification

- **Classification:** Targeted refactor recommended (not full rewrite).
- **Rationale:**
  1. Architecture is modular by stage and mostly recoverable.
  2. Highest-value fixes are focused (auto-run variable mismatch, test expectation drift, documentation/return-style consistency).
  3. No evidence from static review that a complete repository rewrite is required.

## Recommended next authorized mutation batch
1. Fix clean-harmonize auto wrapper variable usage bug.
2. Align `test_run_pipeline` expected sourced scripts with current 4-stage orchestration.
3. Add missing roxygen for `coerce_numeric_safe()`.
4. Replace skipped TODO tests with deterministic unit coverage for setup and file IO helpers.
5. Optionally normalize metadata docs (`README`) to match actual non-package script-repo layout or add missing package metadata files.
