# Pipeline Performance Report: perf_2-postpro_pipeline

- Stage identifier: 2-postpro
- Preset: standard
- Total functions benchmarked: 3
- Dominant complexity class: O(n)
- Stage runtime share: 8.4%
- Stage risk score: 0.2004
- Highest-complexity function: aggregate_standardized_rows (O(n))
- Primary bottleneck candidate: aggregate_standardized_rows (O(n), 61.7% impact)
- Runtime projection sample n values: 1000, 100000, 10000000

## Stage KPI Dashboard

```text
| KPI                         | Value                                      |
| --------------------------- | ------------------------------------------ |
| Total functions benchmarked | 3                                          |
| Dominant complexity class   | O(n)                                       |
| Stage runtime total (s)     | 45.987                                     |
| Stage runtime share         | 8.4%                                       |
| Expensive function share    | 0.0%                                       |
| Stage risk score            | 0.2004                                     |
| Top runtime driver          | aggregate_standardized_rows (61.7%)        |
| Top composite bottleneck    | aggregate_standardized_rows (score 0.3359) |
```

## Function-Level Performance Matrix

```text
| Function                    | Description                                          | Complexity | adj.R2   | Slope per n    | Estimated runtime (sample n)                         | Relative impact | Indicator       | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags           | Dominant in stage | Likely slowdown drivers                              |
| --------------------------- | ---------------------------------------------------- | ---------- | -------- | -------------- | ---------------------------------------------------- | --------------- | --------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | --------------- | ----------------- | ---------------------------------------------------- |
| aggregate_standardized_rows | group-sum n rows by all non-value columns (2% dupli~ | O(n)       | 0.999847 | 0.00000252304  | n=1000: 0.002523 s; n=100000: 0.2523 s; n=10000000:~ | 61.7%           | ! (high_impact) | no         | 3               | very high  | 28.366                     | 25.277               | 0.11549       | 1.247         | 10.98      | 0.3359          | high_impact     | yes               | grouping across repeated keys increases hash/scan c~ |
| apply_standardize_rules     | apply prepared unit conversion rules to an n-row lo~ | O(n)       | 0.98908  | 0.000000956401 | n=1000: 0.0009564 s; n=100000: 0.09564 s; n=1000000~ | 25.2%           | ? (watch)       | no         | 3               | very high  | 11.588                     | 9.6917               | 0.33129       | 2.056         | 29.99      | 0.2098          | high_volatility | yes               | grouping across repeated keys increases hash/scan c~ |
| extract_aggregated_rows     | extract duplicate groups from n-row standardized ta~ | O(n)       | 0.999841 | 0.0000005407   | n=1000: 0.0005407 s; n=100000: 0.05407 s; n=1000000~ | 13.1%           | ? (watch)       | no         | 3               | very high  | 6.033                      | 5.4164               | 0.40185       | 2.312         | 10.81      | 0.1659          | high_volatility | yes               | grouping across repeated keys increases hash/scan c~ |
```

## Bottleneck Candidates

- aggregate_standardized_rows: class O(n), score 0.3359, impact 61.7%, flags high_impact.
- apply_standardize_rules: class O(n), score 0.2098, impact 25.2%, flags high_volatility.
- extract_aggregated_rows: class O(n), score 0.1659, impact 13.1%, flags high_volatility.

## Top Bottlenecks by Composite Score

```text
| Rank | Function                    | Complexity | Composite score | Stage impact | adj.R2  | Confidence | Volatility cv | Growth max | Flags           | Likely slowdown drivers                              |
| ---- | --------------------------- | ---------- | --------------- | ------------ | ------- | ---------- | ------------- | ---------- | --------------- | ---------------------------------------------------- |
| 1    | aggregate_standardized_rows | O(n)       | 0.3359          | 61.7%        | 0.99985 | very high  | 0.11549       | 10.98      | high_impact     | grouping across repeated keys increases hash/scan c~ |
| 2    | apply_standardize_rules     | O(n)       | 0.2098          | 25.2%        | 0.98908 | very high  | 0.33129       | 29.99      | high_volatility | grouping across repeated keys increases hash/scan c~ |
| 3    | extract_aggregated_rows     | O(n)       | 0.1659          | 13.1%        | 0.99984 | very high  | 0.40185       | 10.81      | high_volatility | grouping across repeated keys increases hash/scan c~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | 0.99626 |
| Median adjusted R2          | 0.99984 |
| Low-confidence functions    | 0       |
| Low-confidence share        | 0.0%    |
| High-volatility functions   | 2       |
| High-volatility share       | 66.7%   |
| Critical bottlenecks        | 0       |
| Runtime concentration (HHI) | 0.46118 |
```

## Optimization Priority Queue

```text
| Priority tier | Function                    | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags           |
| ------------- | --------------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | --------------- |
| P2            | aggregate_standardized_rows | O(n)       | 0.3359          | 61.7%        | large runtime share; sharp growth jump (max 10.98x)  | up to 61.7% stage runtime | high_impact     |
| P2            | apply_standardize_rules     | O(n)       | 0.2098          | 25.2%        | unstable repeated timings; sharp growth jump (max 2~ | up to 25.2% stage runtime | high_volatility |
| P2            | extract_aggregated_rows     | O(n)       | 0.1659          | 13.1%        | unstable repeated timings; sharp growth jump (max 1~ | up to 13.1% stage runtime | high_volatility |
```

## Stage Narrative

- Runtime is dominated by aggregate_standardized_rows, contributing 61.7% of stage runtime.
- Asymptotic risk is dominated by aggregate_standardized_rows with class O(n).
- Optimize first: aggregate_standardized_rows because large runtime share; sharp growth jump (max 10.98x); expected impact up to 61.7% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| O(n)             | 3              | 100.0%         | 100.0%        | ######################## |
```

## Runtime Share Distribution (ASCII)

```text
| Function                    | Relative impact | Composite score | Distribution             |
| --------------------------- | --------------- | --------------- | ------------------------ |
| aggregate_standardized_rows | 61.7%           | 0.3359          | ###############--------- |
| apply_standardize_rules     | 25.2%           | 0.2098          | ######------------------ |
| extract_aggregated_rows     | 13.1%           | 0.1659          | ###--------------------- |
```

## Runtime Projection Grid

```text
| Function                    | n        | Estimated runtime (s) |
| --------------------------- | -------- | --------------------- |
| aggregate_standardized_rows | 1000     | 0.002523              |
| apply_standardize_rules     | 1000     | 0.0009564             |
| extract_aggregated_rows     | 1000     | 0.0005407             |
| aggregate_standardized_rows | 100000   | 0.2523                |
| apply_standardize_rules     | 100000   | 0.09564               |
| extract_aggregated_rows     | 100000   | 0.05407               |
| aggregate_standardized_rows | 10000000 | 25.23                 |
| apply_standardize_rules     | 10000000 | 9.564                 |
| extract_aggregated_rows     | 10000000 | 5.407                 |
```
