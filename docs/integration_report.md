# repository integration report

## repository audit summary

- orchestration entrypoint is `R/run_pipeline.R` and sequentially sources general, import, and export pipeline scripts.
- pipeline modules are organized under `R/0-general_pipeline`, `R/1-import_pipeline`, and `R/3-export_pipeline`.
- validation and user-facing messaging are predominantly implemented with `checkmate` and `cli`.

## cross-engine risk matrix

| engine | primary risk | severity | notes |
|---|---|---:|---|
| global static repository audit | source-time side effects in run scripts | medium | run scripts can auto-execute when sourced without overriding options |
| dependency governance auditor | runtime install behavior and package attach model | medium | missing dependencies are installed via `renv::install()` at runtime |
| api surface auditor | script-first project, no exported package namespace | low | backward-compatible function defaults are tested in helper suites |
| reproducibility auditor | environment-dependent script sourcing | medium | deterministic tests exist but execution requires r runtime |
| memory analyzer | data.table copies in transformations | low | copies are explicit and targeted by design |
| code quality metrics engine | mixed script and function responsibilities | medium | modularization exists; further separation possible |
| data contract validator | contract checks present but spread across stages | low | contract-oriented tests present in `tests/testthat/data_integrity` |
| call graph impact analyzer | high coupling between run scripts and global env objects | medium | globals `config` and `fao_data_raw` are expected by downstream scripts |

## dependency and api stability summary

- dependencies are centrally enumerated in `required_packages`.
- signatures for top-level orchestration functions are preserved.
- backward compatibility assertions are present for `run_pipeline()` and `run_general_pipeline()`.

## backward compatibility analysis

- no function signatures were changed in this integration step.
- defaults for orchestrator functions remain unchanged.

## documentation and readme compliance summary

- repository README was added with runtime, layout, and execution instructions.
- integration report assembled to capture orchestration-level risks and migration guidance.

## integration and migration notes

- set `options(fao.run_*_pipeline.auto = FALSE)` in test or automation contexts before sourcing run scripts.
- prefer `run_pipeline(show_view = FALSE)` from repository root for deterministic non-interactive runs.


## deprecation lifecycle

- release n: introduce deprecated compatibility wrappers for legacy source-time behavior.
- release n+1: keep wrappers and continue warning with migration guidance.
- release n+2: remove wrappers and require explicit runner calls.
