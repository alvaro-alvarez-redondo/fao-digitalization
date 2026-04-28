# Pipeline Performance Report: perf_2-postpro_pipeline

- Stage identifier: 2-postpro
- Preset: quick
- Total functions benchmarked: 3
- Dominant complexity class: unknown
- Stage runtime share: 0.1%
- Stage risk score: 0.4669
- Highest-complexity function: aggregate_standardized_rows (unknown)
- Primary bottleneck candidate: aggregate_standardized_rows (unknown, 52.2% impact)
- Runtime projection sample n values: 1000, 10000

## Stage KPI Dashboard

```text
| KPI                         | Value                                      |
| --------------------------- | ------------------------------------------ |
| Total functions benchmarked | 3                                          |
| Dominant complexity class   | unknown                                    |
| Stage runtime total (s)     | 0.073593                                   |
| Stage runtime share         | 0.1%                                       |
| Expensive function share    | 100.0%                                     |
| Stage risk score            | 0.4669                                     |
| Top runtime driver          | aggregate_standardized_rows (52.2%)        |
| Top composite bottleneck    | aggregate_standardized_rows (score 0.6326) |
```

## Function-Level Performance Matrix

```text
| Function                    | Description                                          | Complexity | adj.R2 | Slope per n | Estimated runtime (sample n)  | Relative impact | Indicator            | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags                                                | Dominant in stage | Likely slowdown drivers                              |
| --------------------------- | ---------------------------------------------------- | ---------- | ------ | ----------- | ----------------------------- | --------------- | -------------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | ---------------------------------------------------- | ----------------- | ---------------------------------------------------- |
| aggregate_standardized_rows | group-sum n rows by all non-value columns (2% dupli~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 52.2%           | !!! (critical)       | yes        | 7               | unknown    | 0.038384                   | 0.031032             | 0.32695       | 1.771         | 4.221      | 0.6326          | high_complexity/high_impact/low_confidence/high_vol~ | yes               | grouping across repeated keys increases hash/scan c~ |
| extract_aggregated_rows     | extract duplicate groups from n-row standardized ta~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 31.9%           | !! (high_complexity) | no         | 7               | unknown    | 0.023511                   | 0.015536             | 0.086017      | 1.099         | 1.948      | 0.5618          | high_complexity/low_confidence                       | yes               | grouping across repeated keys increases hash/scan c~ |
| apply_standardize_rules     | apply prepared unit conversion rules to an n-row lo~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 15.9%           | !! (high_complexity) | no         | 7               | unknown    | 0.011697                   | 0.0092788            | 0.096412      | 1.049         | 3.836      | 0.5056          | high_complexity/low_confidence                       | yes               | grouping across repeated keys increases hash/scan c~ |
```

## Bottleneck Candidates

- aggregate_standardized_rows: class unknown, score 0.6326, impact 52.2%, flags high_complexity/high_impact/low_confidence/high_volatility/critical_bottleneck.
- extract_aggregated_rows: class unknown, score 0.5618, impact 31.9%, flags high_complexity/low_confidence.
- apply_standardize_rules: class unknown, score 0.5056, impact 15.9%, flags high_complexity/low_confidence.

## Top Bottlenecks by Composite Score

```text
| Rank | Function                    | Complexity | Composite score | Stage impact | adj.R2 | Confidence | Volatility cv | Growth max | Flags                                                | Likely slowdown drivers                              |
| ---- | --------------------------- | ---------- | --------------- | ------------ | ------ | ---------- | ------------- | ---------- | ---------------------------------------------------- | ---------------------------------------------------- |
| 1    | aggregate_standardized_rows | unknown    | 0.6326          | 52.2%        | N/A    | unknown    | 0.32695       | 4.221      | high_complexity/high_impact/low_confidence/high_vol~ | grouping across repeated keys increases hash/scan c~ |
| 2    | extract_aggregated_rows     | unknown    | 0.5618          | 31.9%        | N/A    | unknown    | 0.086017      | 1.948      | high_complexity/low_confidence                       | grouping across repeated keys increases hash/scan c~ |
| 3    | apply_standardize_rules     | unknown    | 0.5056          | 15.9%        | N/A    | unknown    | 0.096412      | 3.836      | high_complexity/low_confidence                       | grouping across repeated keys increases hash/scan c~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | N/A     |
| Median adjusted R2          | N/A     |
| Low-confidence functions    | 3       |
| Low-confidence share        | 100.0%  |
| High-volatility functions   | 1       |
| High-volatility share       | 33.3%   |
| Critical bottlenecks        | 1       |
| Runtime concentration (HHI) | 0.39937 |
```

## Optimization Priority Queue

```text
| Priority tier | Function                    | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags                                                |
| ------------- | --------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | ---------------------------------------------------- |
| P0            | aggregate_standardized_rows | unknown    | 0.6326          | 52.2%        | large runtime share; super-linear growth risk; unst~ | up to 52.2% stage runtime | high_complexity/high_impact/low_confidence/high_vol~ |
| P1            | extract_aggregated_rows     | unknown    | 0.5618          | 31.9%        | super-linear growth risk; low model confidence       | up to 31.9% stage runtime | high_complexity/low_confidence                       |
| P2            | apply_standardize_rules     | unknown    | 0.5056          | 15.9%        | super-linear growth risk; low model confidence; sha~ | up to 15.9% stage runtime | high_complexity/low_confidence                       |
```

## Stage Narrative

- Runtime is dominated by aggregate_standardized_rows, contributing 52.2% of stage runtime.
- Asymptotic risk is dominated by aggregate_standardized_rows with class unknown.
- Optimize first: aggregate_standardized_rows because large runtime share; super-linear growth risk; unstable repeated timings; low model confidence; sharp growth jump (max 4.22x); expected impact up to 52.2% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| unknown          | 3              | 100.0%         | 100.0%        | ######################## |
```

## Runtime Share Distribution (ASCII)

```text
| Function                    | Relative impact | Composite score | Distribution             |
| --------------------------- | --------------- | --------------- | ------------------------ |
| aggregate_standardized_rows | 52.2%           | 0.6326          | #############----------- |
| extract_aggregated_rows     | 31.9%           | 0.5618          | ########---------------- |
| apply_standardize_rules     | 15.9%           | 0.5056          | ####-------------------- |
```

## Runtime Projection Grid

```text
| Function                    | n     | Estimated runtime (s) |
| --------------------------- | ----- | --------------------- |
| aggregate_standardized_rows | 1000  | N/A                   |
| apply_standardize_rules     | 1000  | N/A                   |
| extract_aggregated_rows     | 1000  | N/A                   |
| aggregate_standardized_rows | 10000 | N/A                   |
| apply_standardize_rules     | 10000 | N/A                   |
| extract_aggregated_rows     | 10000 | N/A                   |
```
