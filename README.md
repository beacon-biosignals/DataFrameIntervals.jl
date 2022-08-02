# DataFrameIntervals

[![CI](https://github.com/beacon-biosignals/DataFrameIntervals.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/beacon-biosignals/DataFrameIntervals.jl/actions/workflows/CI.yml)
[![Coverage](https://codecov.io/gh/beacon-biosignals/DataFrameIntervals.jl/branch/main/graph/badge.svg?token=q4x7zu3TeU)](https://codecov.io/gh/beacon-biosignals/DataFrameIntervals.jl)
[![Code Style: YASGuide](https://img.shields.io/badge/code%20style-yas-violet.svg)](https://github.com/jrevels/YASGuide)
[![Docs: Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://beacon-biosignals.github.io/DataFrameIntervals.jl/stable)
[![Docs: Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://beacon-biosignals.github.io/DataFrameIntervals.jl/dev)

DataFrameIntervals provides two functions that are handy for computing joins over intervals
of time: `interval_join` and `groupby_interval_join`, and a helper function called `quantile_windows`. See their doc strings for details.

Rows match in this join if their time spans overlap. The time spans can be represented as

- [`TimeSpan`](https://juliapackages.com/p/timespans) objects 
- [`Interval`](https://juliapackages.com/p/intervals) objects.
- `NamedTuples` with a `start` and `stop` field.

## Example

```julia
using TimeSpans
using DataFrames
using DataFrameIntervals
using Distributions
using Random
using Dates

n = 100
tovalue(x) = Nanosecond(round(Int, x * 1e9))
times = cumsum(rand(MersenneTwister(hash((:dataframe_intervals, 2022_06_01))), Gamma(3, 2), n+1))
spans = TimeSpan.(tovalue.(times[1:(end-1)]), tovalue.(times[2:end]))
df = DataFrame(label = rand(('a':'d'), n), x = rand(n), span = spans)
```

```
100×3 DataFrame
 Row │ label  x          span
     │ Char   Float64    TimeSpan
─────┼─────────────────────────────────────────────────────
   1 │ b      0.0606309  TimeSpan(00:00:05.164631882, 00:…
   2 │ a      0.961599   TimeSpan(00:00:08.853504418, 00:…
   3 │ c      0.55525    TimeSpan(00:00:13.431519652, 00:…
   4 │ d      0.058248   TimeSpan(00:00:25.929078264, 00:…
  ⋮  │   ⋮        ⋮                      ⋮
  98 │ a      0.995222   TimeSpan(00:08:51.512608520, 00:…
  99 │ d      0.188141   TimeSpan(00:08:56.662988067, 00:…
 100 │ a      0.338053   TimeSpan(00:08:58.445446762, 00:…
 ```

```julia
quarters = quantile_windows(4, df, label=:quarter)

interval_join(df, quarters, on=:span)
```

```
103×6 DataFrame
 Row │ quarter  label  x          span_left                          span_right                         span                              
     │ Int64    Char   Float64    TimeSpan                           TimeSpan                           TimeSpan                          
─────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   1 │       1  b      0.0606309  TimeSpan(00:00:05.164631882, 00:…  TimeSpan(00:00:05.164631882, 00:…  TimeSpan(00:00:05.164631882, 00:…
   2 │       1  a      0.961599   TimeSpan(00:00:08.853504418, 00:…  TimeSpan(00:00:05.164631882, 00:…  TimeSpan(00:00:08.853504418, 00:…
   3 │       1  c      0.55525    TimeSpan(00:00:13.431519652, 00:…  TimeSpan(00:00:05.164631882, 00:…  TimeSpan(00:00:13.431519652, 00:…
   4 │       1  d      0.058248   TimeSpan(00:00:25.929078264, 00:…  TimeSpan(00:00:05.164631882, 00:…  TimeSpan(00:00:25.929078264, 00:…
  ⋮  │    ⋮       ⋮        ⋮                      ⋮                                  ⋮                                  ⋮
 101 │       4  a      0.995222   TimeSpan(00:08:51.512608520, 00:…  TimeSpan(00:06:51.442142229, 00:…  TimeSpan(00:08:51.512608520, 00:…
 102 │       4  d      0.188141   TimeSpan(00:08:56.662988067, 00:…  TimeSpan(00:06:51.442142229, 00:…  TimeSpan(00:08:56.662988067, 00:…
 103 │       4  a      0.338053   TimeSpan(00:08:58.445446762, 00:…  TimeSpan(00:06:51.442142229, 00:…  TimeSpan(00:08:58.445446762, 00:…
```

## Related Packages

Below is a list of related packages and a brief indication of their differences from `DataFrameIntervals`.

- [TSx](https://github.com/xKDR/TSx.jl) various operations on time series data: includes many features DataFrameIntervals does not aim to implement. Does not implement joins over intervals of time.
- [FlexiJoins](https://gitlab.com/aplavin/FlexiJoins.jl) generic join operations, including by interval predicates (`∈, ⊆, ⊊, ⊋, ⊇, !isdisjoint`): the algorithms applied here are more general purpose and are bound by the complexity of more general purpose data structures (e.g. KD-trees). DataFrameIntervals is (currently) bound by a lower complexity class for its specific use case.
- [InMemoryDatasets.jl](https://sl-solution.github.io/InMemoryDatasets.jl/stable/man/joins/#Inequality-kind-joins) includes inequality-like joins over intervals of time (where the interval is represented as two columns); this cannot yet achieve the behavior implemented in `DataFrameIntervals`, where multiple inequalities must be checked to determine overlap.