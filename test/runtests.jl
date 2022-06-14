using DataFrameIntervals
using Intervals
using DataFrames
using TimeSpans
using Test
using Random
using Dates
using Distributions
using Aqua

# some light type piracy
Base.isapprox(a::TimePeriod, b::TimePeriod; atol=period) = return abs(a - b) ≤ atol

@testset "DataFrameIntervals.jl" begin
    n = 100
    tovalue(x) = Nanosecond(round(Int, x * 1e9))
    times = cumsum(rand(MersenneTwister(hash((:dataframe_intervals, 2022_06_01))), Gamma(3, 2), n+1))
    spans = TimeSpan.(tovalue.(times[1:(end-1)]), tovalue.(times[2:end]))
    df1 = DataFrame(label = rand(('a':'d'), n), x = rand(n), span = spans)
    quarters = quantile_windows(4, df1, label=:quarter)
    @test nrow(quarters) == 4
    @test isapprox(duration(quarters.span[1]), duration(quarters.span[2]), atol=Nanosecond(1)) 
    @test isapprox(duration(quarters.span[2]), duration(quarters.span[3]), atol=Nanosecond(1)) 
    @test isapprox(duration(quarters.span[2]), duration(quarters.span[3]), atol=Nanosecond(1)) ||
          duration(quarters.span[4]) ≤ duration(quarters.span[3])

    # NOTE: the bulk of the correctness testing for interval intersections
    # has already been handled by `Intervals.find_intervals`
    df_result = split_into(df1, quarters)
    for quarter in groupby(df_result, :right_span)
        @test sum(duration, quarter.span) ≤ duration(quarter.right_span[1])
    end
    ixs = Intervals.find_intersections(DataFrameIntervals.interval.(quarters.span), 
                                       DataFrameIntervals.interval.(df1.span))
    @test df_result.left_span == mapreduce(ix -> df1.span[ix], vcat, ixs)
    
    # split_into_combine equivalence
    df_combined = split_into_combine(df1, quarters, [:quarter, :label], :x => mean)
    df_manual_combined = combine(groupby(split_into(df1, quarters), [:quarter, :label]), :x => mean)
    @test df_combined.x_mean == df_manual_combined.x_mean

    # test out various column specifiers
    df_combined = split_into_combine(df1, quarters, r"quar|lab", :x => mean)
    df_combined = split_into_combine(df1, quarters, Cols(:quarter, r"lab"), :x => mean)
    df_combined = split_into_combine(df1, quarters, Not([:span, :x]), :x => mean)
    err = ErrorException("Column span cannot be used for grouping during a call to `split_into_combine`.")
    @test_throws err split_into_combine(df1, quarters, All())
    # TODO: resolve this error
    @test_throws err split_into_combine(df1, quarters, Cols(:))

    df2 = DataFrame(label = rand(('a':'d'), n), sublabel = rand(('k':'n'), n), x = rand(n), span = spans)
    @test split_into_combine(df2, quarters, Cols(Between(:label, :sublabel), :quarter), :x => mean)
    @test_throws ErrorException split_into_combine(df2, quarters, [:i_dont_exist], :x => mean)
    @test_throws ErrorException split_into_combine(df2, quarters, Cols(1:2), :x => mean)

    Aqua.test_all(DataFrameIntervals; 
                  project_extras=false,
                  stale_deps=true,
                  deps_compat=true,
                  project_toml_formatting=true,
                  ambiguities=false)
end
