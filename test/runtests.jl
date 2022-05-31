using DataFrameIntervals
using Intervals
using DataFrames
using TimeSpans
using Test

@testset "DataFrameIntervals.jl" begin
    n = 10
    tovalue(x) = Nanosecond(round(Int, x * 1e9))
    times = cumsum(nrand(n+1))
    spans = TimeSpan.(tovalue.(times[1:(end-1)]), tvalue.(times[2:end]))
    df1 = DataFrame(label = rand(('a':'d'), n), x = rand(n), span = spans)
    df2 = quantile_windows(4, superset(spans))

    # TODO: wait for Intervlas 1.7 to merge; then run this and build tests
    # to verify that outputs match what we'd expect with manual use of `find_intersections`
end
