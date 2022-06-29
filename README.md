# DataFrameIntervals

[![Build Status](https://github.com/haberdashpi/DataFrameIntervals.jl/actions/workflows/CI.yml/badge.svg?branch=)](https://github.com/biosignals/DataFrameIntervals.jl/actions/workflows/CI.yml?query=branch%3A)
[![Coverage](https://codecov.io/gh/beacon-biosignals/DataFrameIntervals.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/biosignals/DataFrameIntervals.jl)
[![Code Style: YASGuide](https://img.shields.io/badge/code%20style-yas-violet.svg)](https://github.com/jrevels/YASGu)
[![Docs: Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://beacon-biosignals.github.io/DataFrameIntervals.jl/stable)
[![Docs: Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://beacon-biosignals.github.io/DataFrameIntervals.jl/dev)

DataFrameIntervals provides two functions that are handy for computing joins over intervals
of time: split_into and split_into_combine, and a helper function called `quantile_windows`.

Rows match in this join if their time spans overlap. The time spans can be represented as i[`TimeSpan`](https://juliapackages.com/p/timespans) objects or [`Interval`](https://juliapackages.com/p/intervals) objects.

Currently this requires an unreleased version of `Intervals.jl` (which should be version 1.8 when released). Make sure to add the following to your project before adding `DataFrameIntervals`.

```
julia> ]add https://github.com/invenia/Intervals.jl#rf/intervalset-type
```


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

split_into(df, quarters)
```

```
103×6 DataFrame
 Row │ quarter  label  x          left_span                          right_span                         span
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
