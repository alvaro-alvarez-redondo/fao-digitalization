---
name: hard-coding
description: 'Centralize all hard-coded literals into 01-setup.R.'
argument-hint: Which hard-coded values should be centralized?
agent: agent
---

## Mandatory Actions
- Scan repository for paths, thresholds, URLs, magic numbers, repeated strings, environment settings.
- Refactor scripts to reference constants in `01-setup.R`.
- Ensure modernized behavior and eliminate backward compatibility constraints.
- Ensure tests pass.

## Constraints
- Preserve API surface.
- Deterministic behavior only.