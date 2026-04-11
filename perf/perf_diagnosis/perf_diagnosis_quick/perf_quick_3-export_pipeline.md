# Pipeline Performance Report: perf_3-export_pipeline

- Stage identifier: 3-export
- Preset: quick
- Total functions benchmarked: 2
- Dominant complexity class: unknown
- Stage runtime share: 0.0%
- Stage risk score:   0.5
- Highest-complexity function: normalize_for_comparison (unknown)
- Primary bottleneck candidate: normalize_for_comparison (unknown, 91.5% impact)
- Runtime projection sample n values: 1000, 10000

## Stage KPI Dashboard

```text
| KPI                         | Value                                   |
| --------------------------- | --------------------------------------- |
| Total functions benchmarked | 2                                       |
| Dominant complexity class   | unknown                                 |
| Stage runtime total (s)     | 0.010049                                |
| Stage runtime share         | 0.0%                                    |
| Expensive function share    | 100.0%                                  |
| Stage risk score            | 0.5                                     |
| Top runtime driver          | normalize_for_comparison (91.5%)        |
| Top composite bottleneck    | normalize_for_comparison (score 0.7703) |
```

## Function-Level Performance Matrix

```text
| Function                     | Description                                          | Complexity | adj.R2 | Slope per n | Estimated runtime (sample n)  | Relative impact | Indicator            | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags                                                | Dominant in stage | Likely slowdown drivers                              |
| ---------------------------- | ---------------------------------------------------- | ---------- | ------ | ----------- | ----------------------------- | --------------- | -------------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | ---------------------------------------------------- | ----------------- | ---------------------------------------------------- |
| normalize_for_comparison     | deep-copy, drop year col, sort columns and rows of ~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 91.5%           | !!! (critical)       | yes        | 7               | unknown    | 0.0091977                  | 0.0075766            | 0.081379      | 1.129         | 4.674      | 0.7703          | high_complexity/high_impact/low_confidence/critical~ | yes               | sorting/reordering work adds comparison overhead; t~ |
| compute_unique_column_values | compute sorted unique values for one column of n-ro~ | unknown    | N/A    | N/A         | n=1000: N/A s; n=10000: N/A s | 8.5%            | !! (high_complexity) | no         | 7               | unknown    | 0.0008516                  | 0.0004511            | 0.2741        | 1.424         | 1.126      | 0.4797          | high_complexity/low_confidence/high_volatility       | yes               | sorting/reordering work adds comparison overhead; s~ |
```

## Bottleneck Candidates

- normalize_for_comparison: class unknown, score 0.7703, impact 91.5%, flags high_complexity/high_impact/low_confidence/critical_bottleneck.
- compute_unique_column_values: class unknown, score 0.4797, impact 8.5%, flags high_complexity/low_confidence/high_volatility.

## Top Bottlenecks by Composite Score

```text
| Rank | Function                     | Complexity | Composite score | Stage impact | adj.R2 | Confidence | Volatility cv | Growth max | Flags                                                | Likely slowdown drivers                              |
| ---- | ---------------------------- | ---------- | --------------- | ------------ | ------ | ---------- | ------------- | ---------- | ---------------------------------------------------- | ---------------------------------------------------- |
| 1    | normalize_for_comparison     | unknown    | 0.7703          | 91.5%        | N/A    | unknown    | 0.081379      | 4.674      | high_complexity/high_impact/low_confidence/critical~ | sorting/reordering work adds comparison overhead; t~ |
| 2    | compute_unique_column_values | unknown    | 0.4797          | 8.5%         | N/A    | unknown    | 0.2741        | 1.126      | high_complexity/low_confidence/high_volatility       | sorting/reordering work adds comparison overhead; s~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | N/A     |
| Median adjusted R2          | N/A     |
| Low-confidence functions    | 2       |
| Low-confidence share        | 100.0%  |
| High-volatility functions   | 1       |
| High-volatility share       | 50.0%   |
| Critical bottlenecks        | 1       |
| Runtime concentration (HHI) | 0.84488 |
```

## Optimization Priority Queue

```text
| Priority tier | Function                     | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags                                                |
| ------------- | ---------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | ---------------------------------------------------- |
| P0            | normalize_for_comparison     | unknown    | 0.7703          | 91.5%        | large runtime share; super-linear growth risk; low ~ | up to 91.5% stage runtime | high_complexity/high_impact/low_confidence/critical~ |
| P2            | compute_unique_column_values | unknown    | 0.4797          | 8.5%         | super-linear growth risk; unstable repeated timings~ | up to 8.5% stage runtime  | high_complexity/low_confidence/high_volatility       |
```

## Stage Narrative

- Runtime is dominated by normalize_for_comparison, contributing 91.5% of stage runtime.
- Asymptotic risk is dominated by normalize_for_comparison with class unknown.
- Optimize first: normalize_for_comparison because large runtime share; super-linear growth risk; low model confidence; sharp growth jump (max 4.67x); expected impact up to 91.5% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| unknown          | 2              | 100.0%         | 100.0%        | ######################## |
```

## Runtime Share Distribution (ASCII)

```text
| Function                     | Relative impact | Composite score | Distribution             |
| ---------------------------- | --------------- | --------------- | ------------------------ |
| normalize_for_comparison     | 91.5%           | 0.7703          | ######################-- |
| compute_unique_column_values | 8.5%            | 0.4797          | ##---------------------- |
```

## Runtime Projection Grid

```text
| Function                     | n     | Estimated runtime (s) |
| ---------------------------- | ----- | --------------------- |
| compute_unique_column_values | 1000  | N/A                   |
| normalize_for_comparison     | 1000  | N/A                   |
| compute_unique_column_values | 10000 | N/A                   |
| normalize_for_comparison     | 10000 | N/A                   |
```
