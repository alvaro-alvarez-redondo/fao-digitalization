# dependency governance audit

## 1. dependency map

### declared dependency registry

`R/0-general_pipeline/00-dependencies.R` defines a single dependency vector used by the runtime bootstrapping flow:

- tidyverse
- data.table
- readxl
- janitor
- openxlsx
- fs
- here
- stringi
- progressr
- renv
- testthat
- checkmate
- cli
- progress

### runtime dependency control flow

- `run_general_pipeline()` calls `check_dependencies(required_packages)` and then `load_dependencies(required_packages)`.
- `check_dependencies()` validates names and installs missing packages with `renv::install()`.
- `load_dependencies()` attaches packages through `require()` in a `purrr::walk()` loop.

### observed namespace usage across repository

Static scan across `R/`, `tests/`, and `benchmarks/` shows explicit usage for:

- bench
- checkmate
- cli
- data.table
- dplyr
- fs
- here
- openxlsx
- progressr
- purrr
- readr
- readxl
- renv
- stringi
- stringr
- testthat
- tibble
- tidyr
- tidyselect
- utils
- withr

## 2. risk classification

| risk area | severity | notes |
|---|---|---|
| lockfile governance | critical | `renv.lock` and renv project bootstrap files are missing, so dependency graph pinning and drift checks are not enforceable. |
| imports vs suggests separation | critical | there is no `DESCRIPTION`/`NAMESPACE`, so runtime and development dependency boundaries are not formally declared. |
| namespace leakage | major | `load_dependencies()` globally attaches each dependency using `require()`, increasing accidental symbol masking and hidden coupling risk. |
| transitive dependency surface | major | `tidyverse` introduces a large umbrella dependency while code already relies on specific namespaces (`dplyr`, `purrr`, `tidyr`, etc.). |
| runtime environment mutation | major | `check_dependencies()` installs packages at runtime through `renv::install()`, which can diverge by machine/session state. |
| duplicate/test dependency in runtime registry | minor | `testthat` is included in runtime bootstrapping even though it is primarily a test dependency. |
| `:::` misuse | low | no `:::` usage detected in repository scan. |

## 3. unused dependency list

From `required_packages` compared to explicit namespace calls in repository code:

- tidyverse (umbrella package not directly called as `tidyverse::`)
- janitor (no explicit `janitor::` usage found)
- progress (no explicit `progress::` usage found)

Potential misclassification (used, but likely test-only):

- testthat

## 4. optimization recommendations

1. replace umbrella dependency usage by explicit package dependencies only, and remove `tidyverse` from runtime registry if not required as attach shortcut.
2. remove unused dependencies from `required_packages` (`janitor`, `progress`) unless future modules depend on them.
3. separate runtime and test dependencies (for example, runtime list + test list) and avoid loading `testthat` in pipeline execution.
4. replace attach-based loading with explicit namespace use (`pkg::fn`) where possible to reduce search-path side effects.
5. move installation behavior out of normal pipeline execution, favoring a pre-run environment bootstrap (`renv::restore()`) workflow.
6. add package governance artifacts (`DESCRIPTION`, `NAMESPACE`) and restoreable lock state (`renv.lock`, `renv/activate.R`, settings file).

## 5. lockfile integrity assessment

status: **failed / not assessable**.

findings:

- `renv.lock`: missing
- `renv/activate.R`: missing
- `renv/settings.json`: missing
- `DESCRIPTION`: missing
- `NAMESPACE`: missing

Because lock metadata is absent, version conflict analysis is limited to coarse static risk (no pinned versions to compare, no deterministic snapshot validation).

## 6. all scripts edited by the dependency governance auditor

- `docs/dependency_governance_audit.md`
