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
    df_result = interval_join(df1, quarters)
    for quarter in groupby(df_result, :right_span)
        @test sum(duration, quarter.span) ≤ duration(quarter.right_span[1])
    end
    ixs = Intervals.find_intersections(DataFrameIntervals.interval.(quarters.span), 
                                       DataFrameIntervals.interval.(df1.span))
    @test df_result.left_span == mapreduce(ix -> df1.span[ix], vcat, ixs)
    
    # split_into_combine equivalence
    df_combined = combine(groupby_interval_join(df1, quarters, [:quarter, :label]), :x => mean)
    df_manual_combined = combine(groupby(interval_join(df1, quarters), [:quarter, :label]), :x => mean)
    @test df_combined.x_mean == df_manual_combined.x_mean

    df_grouped1 = groupby(interval_join(df, quarters), [:quarter, :label])
    df_grouped2 = groupby_interval_join(df, quarters, [:quarter, :label])
    for (gdf1, gdf2) in zip(df_grouped1, df_grouped2)
        gdf1.x_mean == gdf2.x_mean
    end

    # test out various column specifiers
    df_combined = combine(groupby_interval_join(df1, quarters, r"quar|lab"), :x => mean)
    df_combined = combine(groupby_interval_join(df1, quarters, Cols(:quarter, r"lab")), :x => mean)
    df_combined = combine(groupby_interval_join(df1, quarters, Not([:span, :x])), :x => mean)
    err = ErrorException("Column span cannot be used for grouping during a call to `split_into_combine`.")
    @test_throws err combine(groupby_interval_join(df1, quarters, All()), :x => mean)
    @test_throws err combine(groupby_interval_join(df1, quarters, Cols(:)), :x => mean)

    df2 = DataFrame(label = rand(('a':'d'), n), sublabel = rand(('k':'n'), n), x = rand(n), span = spans)
    df2_split = combine(groupby_interval_join(df2, quarters), Cols(Between(:label, :sublabel), :quarter), :x => mean)
    df2_manual = combine(groupby(interval_join(df2, quarters), Cols(Between(:label, :sublabel), :quarter)), :x => mean)
    @test df2_split.x_mean == df2_manual.x_mean
    @test_throws ErrorException combine(groupby_interval_join(df2, quarters), [:i_dont_exist], :x => mean)
    @test_throws ErrorException split_into_combine(df2, quarters, Cols(1:2), :x => mean)


    @testset "Code Quality" begin
        Aqua.test_all(DataFrameIntervals; 
                    project_extras=true,
                    stale_deps=true,
                    deps_compat=true,
                    project_toml_formatting=true,
                    ambiguities=false)
    end
end
