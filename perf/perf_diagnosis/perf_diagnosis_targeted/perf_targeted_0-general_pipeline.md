# Pipeline Performance Report: perf_0-general_pipeline

- Stage identifier: 0-general
- Preset: targeted
- Total functions benchmarked: 3
- Dominant complexity class: O(n log n)
- Stage runtime share: 12.0%
- Stage risk score: 0.2646
- Highest-complexity function: drop_na_value_rows (O(n log n))
- Primary bottleneck candidate: normalize_string_impl (O(n), 35.8% impact)
- Runtime projection sample n values: 1000, 10000, 1000000

## Stage KPI Dashboard

```text
| KPI                         | Value                                |
| --------------------------- | ------------------------------------ |
| Total functions benchmarked | 3                                    |
| Dominant complexity class   | O(n log n)                           |
| Stage runtime total (s)     | 0.62832                              |
| Stage runtime share         | 12.0%                                |
| Expensive function share    | 0.0%                                 |
| Stage risk score            | 0.2646                               |
| Top runtime driver          | coerce_numeric_safe (40.9%)          |
| Top composite bottleneck    | normalize_string_impl (score 0.4452) |
```

## Function-Level Performance Matrix

```text
| Function              | Description                                          | Complexity | adj.R2   | Slope per n     | Estimated runtime (sample n)                         | Relative impact | Indicator       | Bottleneck | Complexity rank | Confidence | Observed runtime total (s) | Runtime at max n (s) | Volatility cv | p99/p50 ratio | Growth max | Composite score | Flags                       | Dominant in stage | Likely slowdown drivers                              |
| --------------------- | ---------------------------------------------------- | ---------- | -------- | --------------- | ---------------------------------------------------- | --------------- | --------------- | ---------- | --------------- | ---------- | -------------------------- | -------------------- | ------------- | ------------- | ---------- | --------------- | --------------------------- | ----------------- | ---------------------------------------------------- |
| normalize_string_impl | normalise a character vector of length n to ASCII l~ | O(n)       | 0.999888 | 0.000000193577  | n=1000: 0.0001936 s; n=10000: 0.001936 s; n=1000000~ | 35.8%           | ! (high_impact) | no         | 3               | very high  | 0.22465                    | 0.196                | 0.23207       | 1.48          | 9.182      | 0.4452          | high_impact/high_volatility | no                | string normalization/coercion is CPU and allocation~ |
| coerce_numeric_safe   | coerce a character vector of length n to numeric     | O(n)       | 0.999804 | 0.000000235336  | n=1000: 0.0002353 s; n=10000: 0.002353 s; n=1000000~ | 40.9%           | ! (high_impact) | no         | 3               | very high  | 0.25673                    | 0.23433              | 0.026234      | 1.035         | 12.05      | 0.263           | high_impact                 | no                | string normalization/coercion is CPU and allocation~ |
| drop_na_value_rows    | filter NA-value rows from a data.table of n rows (5~ | O(n log n) | 0.997363 | 0.0000000101742 | n=1000: 0.00007028 s; n=10000: 0.0009371 s; n=10000~ | 23.4%           | ? (watch)       | no         | 4               | very high  | 0.14695                    | 0.13931              | 0.70219       | 2.363         | 23.32      | 0.2623          | high_volatility             | yes               | runtime jumps sharply between sizes (max 23.32x); h~ |
```

## Bottleneck Candidates

- normalize_string_impl: class O(n), score 0.4452, impact 35.8%, flags high_impact/high_volatility.
- coerce_numeric_safe: class O(n), score 0.263, impact 40.9%, flags high_impact.
- drop_na_value_rows: class O(n log n), score 0.2623, impact 23.4%, flags high_volatility.

## Top Bottlenecks by Composite Score

```text
| Rank | Function              | Complexity | Composite score | Stage impact | adj.R2  | Confidence | Volatility cv | Growth max | Flags                       | Likely slowdown drivers                              |
| ---- | --------------------- | ---------- | --------------- | ------------ | ------- | ---------- | ------------- | ---------- | --------------------------- | ---------------------------------------------------- |
| 1    | normalize_string_impl | O(n)       | 0.4452          | 35.8%        | 0.99989 | very high  | 0.23207       | 9.182      | high_impact/high_volatility | string normalization/coercion is CPU and allocation~ |
| 2    | coerce_numeric_safe   | O(n)       | 0.263           | 40.9%        | 0.9998  | very high  | 0.026234      | 12.05      | high_impact                 | string normalization/coercion is CPU and allocation~ |
| 3    | drop_na_value_rows    | O(n log n) | 0.2623          | 23.4%        | 0.99736 | very high  | 0.70219       | 23.32      | high_volatility             | runtime jumps sharply between sizes (max 23.32x); h~ |
```

## Confidence and Uncertainty Summary

```text
| Metric                      | Value   |
| --------------------------- | ------- |
| Mean adjusted R2            | 0.99902 |
| Median adjusted R2          | 0.9998  |
| Low-confidence functions    | 0       |
| Low-confidence share        | 0.0%    |
| High-volatility functions   | 2       |
| High-volatility share       | 66.7%   |
| Critical bottlenecks        | 0       |
| Runtime concentration (HHI) | 0.34947 |
```

## Optimization Priority Queue

```text
| Priority tier | Function              | Complexity | Composite score | Stage impact | Reason                                               | Expected impact           | Flags                       |
| ------------- | --------------------- | ---------- | --------------- | ------------ | ---------------------------------------------------- | ------------------------- | --------------------------- |
| P2            | normalize_string_impl | O(n)       | 0.4452          | 35.8%        | large runtime share; unstable repeated timings; sha~ | up to 35.8% stage runtime | high_impact/high_volatility |
| P2            | coerce_numeric_safe   | O(n)       | 0.263           | 40.9%        | large runtime share; sharp growth jump (max 12.05x)  | up to 40.9% stage runtime | high_impact                 |
| P2            | drop_na_value_rows    | O(n log n) | 0.2623          | 23.4%        | unstable repeated timings; sharp growth jump (max 2~ | up to 23.4% stage runtime | high_volatility             |
```

## Stage Narrative

- Runtime is dominated by coerce_numeric_safe, contributing 40.9% of stage runtime.
- Asymptotic risk is dominated by drop_na_value_rows with class O(n log n).
- Optimize first: normalize_string_impl because large runtime share; unstable repeated timings; sharp growth jump (max 9.18x); expected impact up to 35.8% stage runtime.

## Complexity Distribution (ASCII)

```text
| Complexity class | Function count | Function share | Runtime share | Distribution             |
| ---------------- | -------------- | -------------- | ------------- | ------------------------ |
| O(n)             | 2              | 66.7%          | 76.6%         | ##################------ |
| O(n log n)       | 1              | 33.3%          | 23.4%         | ######------------------ |
```

## Runtime Share Distribution (ASCII)

```text
| Function              | Relative impact | Composite score | Distribution             |
| --------------------- | --------------- | --------------- | ------------------------ |
| coerce_numeric_safe   | 40.9%           | 0.263           | ##########-------------- |
| normalize_string_impl | 35.8%           | 0.4452          | #########--------------- |
| drop_na_value_rows    | 23.4%           | 0.2623          | ######------------------ |
```

## Runtime Projection Grid

```text
| Function              | n       | Estimated runtime (s) |
| --------------------- | ------- | --------------------- |
| coerce_numeric_safe   | 1000    | 0.00023534            |
| drop_na_value_rows    | 1000    | 0.000070281           |
| normalize_string_impl | 1000    | 0.00019358            |
| coerce_numeric_safe   | 10000   | 0.0023534             |
| drop_na_value_rows    | 10000   | 0.00093708            |
| normalize_string_impl | 10000   | 0.0019358             |
| coerce_numeric_safe   | 1000000 | 0.23534               |
| drop_na_value_rows    | 1000000 | 0.14056               |
| normalize_string_impl | 1000000 | 0.19358               |
```
