---
name: refactor
description: 'Perform full repository audit and implement refactors.'
argument-hint: What audit and refactor rules should apply?
agent: agent
---

## Purpose
Audit and modernize all R scripts, dependencies, tests, and folder structure. Fix any legacy patterns.

## Mandatory Actions
- Enforce `snake_case`, native pipe `|>`, `<-` assignment.
- Remove global state; add `checkmate` validation; explicit `return()`.
- Reduce duplication; improve modularity
- Stabilize output schema.
- Remove unused dependencies; correct imports; fix `renv` inconsistencies.
- Improve test coverage.
- Eliminate backward compatibility constraints.