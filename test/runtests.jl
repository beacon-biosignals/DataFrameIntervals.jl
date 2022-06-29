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
    times = cumsum(rand(MersenneTwister(hash((:dataframe_intervals, 2022_06_01))),
                        Gamma(3, 2), n + 1))
    spans = TimeSpan.(tovalue.(times[1:(end - 1)]), tovalue.(times[2:end]))
    df1 = DataFrame(; label=rand(('a':'d'), n), x=rand(n), span=spans)
    quarters = quantile_windows(4, df1; label=:quarter)
    @test nrow(quarters) == 4
    @test isapprox(duration(quarters.span[1]), duration(quarters.span[2]),
                   atol=Nanosecond(1))
    @test isapprox(duration(quarters.span[2]), duration(quarters.span[3]),
                   atol=Nanosecond(1))
    @test isapprox(duration(quarters.span[2]), duration(quarters.span[3]);
                   atol=Nanosecond(1)) ||
          duration(quarters.span[4]) ≤ duration(quarters.span[3])
    @test nrow(quantile_windows(4, subset(df1, :label => ByRow(in('a':'b'))))) == 4

    

    # NOTE: the bulk of the correctness testing for interval intersections
    # has already been handled by calling out to `Intervals.find_intervals`
    # which has been tested in `Intervals.jl`
    df_result = interval_join(df1, quarters; on=:span)
    for quarter in groupby(df_result, :span_right)
        @test sum(duration, quarter.span) ≤ duration(quarter.span_right[1])
    end
    ixs = Intervals.find_intersections(DataFrameIntervals.interval.(quarters.span),
                                       DataFrameIntervals.interval.(df1.span))
    @test df_result.span_left == mapreduce(ix -> df1.span[ix], vcat, ixs)

    # test interval joins with named tuples
    nt_spans = [(; start=start(x), stop=stop(x)) for x in spans]
    df1_nt = hcat(df1[!, Not(:span)], DataFrame(; span=nt_spans))
    df_result_nt = interval_join(df1_nt, quarters; on=:span)
    @test nrow(df_result_nt) == nrow(df_result)

    # groubpy_interval_join equivalence
    df_combined = combine(groupby_interval_join(df1, quarters, [:quarter, :label];
                                                on=:span), :x => mean)
    df_manual_combined = combine(groupby(interval_join(df1, quarters; on=:span),
                                         [:quarter, :label]), :x => mean)
    @test df_combined.x_mean == df_manual_combined.x_mean

    df_grouped1 = groupby(interval_join(df1, quarters; on=:span), [:quarter, :label])
    df_grouped2 = groupby_interval_join(df1, quarters, [:quarter, :label]; on=:span)
    for (gdf1, gdf2) in zip(df_grouped1, df_grouped2)
        @test gdf1.x == gdf2.x
    end

    # test out various column specifiers
    df_combined = combine(groupby_interval_join(df1, quarters, r"quar|lab"; on=:span),
                          :x => mean)
    df_combined = combine(groupby_interval_join(df1, quarters, Cols(:quarter, r"lab");
                                                on=:span), :x => mean)
    df_combined = combine(groupby_interval_join(df1, quarters, Not([:span, :x]); on=:span),
                          :x => mean)
    err = ErrorException("Column span cannot be used for grouping during a call to `split_into_combine`.")
    @test_throws err combine(groupby_interval_join(df1, quarters, All(); on=:span),
                             :x => mean)
    @test_throws err combine(groupby_interval_join(df1, quarters, Cols(:); on=:span),
                             :x => mean)

    df2 = DataFrame(; label=rand(('a':'d'), n), sublabel=rand(('k':'n'), n), x=rand(n),
                    span=spans)
    df2_split = combine(groupby_interval_join(df2, quarters,
                                              Cols(Between(:label, :sublabel), :quarter);
                                              on=:span),
                        :x => mean)
    df2_manual = combine(groupby(interval_join(df2, quarters; on=:span),
                                 Cols(Between(:label, :sublabel), :quarter)), :x => mean)
    @test df2_split.x_mean == df2_manual.x_mean
    @test_throws ErrorException combine(groupby_interval_join(df2, quarters,
                                                              [:i_dont_exist]; on=:span),
                                        :x => mean)
    @test_throws ErrorException combine(groupby_interval_join(df2, quarters, Cols(1:2);
                                                              on=:span), :x => mean)

    @testset "Code Quality" begin
        Aqua.test_all(DataFrameIntervals;
                      project_extras=true,
                      stale_deps=true,
                      deps_compat=true,
                      project_toml_formatting=true,
                      ambiguities=false)
    end
end
