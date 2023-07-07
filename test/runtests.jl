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
    # setup
    n = 100
    tovalue(x) = Nanosecond(round(Int, x * 1e9))
    times = cumsum(rand(MersenneTwister(hash((:dataframe_intervals, 2022_06_01))),
                        Gamma(3, 2), n + 1))
    spans = TimeSpan.(tovalue.(times[1:(end - 1)]), tovalue.(times[2:end]))
    df1 = DataFrame(; label=rand(('a':'d'), n), x=rand(n), span=spans)
    quarters = quantile_windows(4, df1; label=:quarter)
    df_result = interval_join(df1, quarters; on=:span)

    @testset "Basic Interval Joins" begin
        @test nrow(quarters) == 4
        @test isapprox(duration(quarters.span[1]), duration(quarters.span[2]);
                       atol=Nanosecond(1))
        @test isapprox(duration(quarters.span[2]), duration(quarters.span[3]);
                       atol=Nanosecond(1))
        @test isapprox(duration(quarters.span[2]), duration(quarters.span[3]);
                       atol=Nanosecond(1)) ||
              duration(quarters.span[4]) ≤ duration(quarters.span[3])
        @test nrow(quantile_windows(4, subset(df1, :label => ByRow(in('a':'b'))))) == 4

        # NOTE: the bulk of the correctness testing for interval intersections
        # has already been handled by calling out to `Intervals.find_intervals`
        # which has been tested in `Intervals.jl`
        for quarter in groupby(df_result, :span_right)
            @test sum(duration, quarter.span) ≤ duration(quarter.span_right[1])
        end
        ixs = Intervals.find_intersections(DataFrameIntervals.interval.(quarters.span),
                                           DataFrameIntervals.interval.(df1.span))
        @test df_result.span_left == mapreduce(ix -> df1.span[ix], vcat, ixs)
        @test names(interval_join(df1, empty(quarters); on=:span)) == names(df_result)
        @test names(interval_join(empty(df1), quarters; on=:span)) == names(df_result)

        # test the handling of missing
        quarter_miss = transform(quarters, :span => (x -> [x[1:(end - 1)]; missing]);
                                 renamecols=false)
        @test_throws ArgumentError interval_join(df1, quarter_miss; on=:span)
    end

    @testset "Left and right joins" begin
        # test `keepleft` and `keepright`
        df_left = interval_join(df1, quarters[1:3, :]; on=:span, keepleft=true)
        df_last_quarter = subset(df_result, :quarter => ByRow(==(4)))[2:end, :]
        @test df_left[ismissing.(df_left.span), :span_left] == df_last_quarter.span_left

        df_no_q4 = select!(dropmissing(df_left, :span), :label, :x, :span_left => :span)[1:(end - 1),
                                                                                         :]
        df_right = interval_join(df_no_q4, quarters; on=:span, keepright=true)

        @test df_right[ismissing.(df_right.span), :span_right] == quarters[4:4, :span]
    end

    @testset "Column renaming" begin
        # test column renaming
        rename!(quarters, :span => :time_span)
        df_result2 = interval_join(df1, quarters; on=:span => :time_span,
                                   renameon=:_a => :_b,
                                   renamecols=:_left => :_right)
        rename!(quarters, :time_span => :span)
        @test issetequal(names(df_result2),
                         ["time_span_b", "quarter_right", "label_left", "x_left", "span_a",
                          "span"])
        quarters_2 = insertcols!(copy(quarters), :label => rand('y':'z', 4))
        df_result3 = interval_join(df1, quarters_2; on=:span, makeunique=true)
        @test issetequal(names(df_result3),
                         ["span_right", "quarter", "label", "label_1", "x",
                          "span_left", "span"])
    end

    @testset "NamedTuples" begin
        # test interval joins with named tuples
        nt_spans = [(; start=start(x), stop=stop(x)) for x in spans]
        df1_nt = hcat(df1[!, Not(:span)], DataFrame(; span=nt_spans))
        df_result_nt = interval_join(df1_nt, quarters; on=:span)
        @test nrow(df_result_nt) == nrow(df_result)
    end

    @testset "Join over multiple columns" begin
        df1_left_right = select(df1, :label, :x,
                                :span => ByRow(x -> (; start=start(x), stop=stop(x))) => AsTable)
        df_result = interval_join(df1_left_right, quarters;
                                  on=((:start, :stop) => TimeSpan) => :span)
        for quarter in groupby(df_result, :span_right)
            @test sum(duration, quarter.span) ≤ duration(quarter.span_right[1])
        end

        quarters_lr = select(quarters, :quarter,
                             :span => ByRow(x -> (; start=start(x), stop=stop(x))) => AsTable)
        df_result_ = interval_join(df1_left_right, quarters_lr;
                                   on=(:start, :stop) => TimeSpan)
        @test nrow(df_result_) == nrow(df_result)

        df_result_ = interval_join(df1, quarters_lr;
                                   on=:span => ((:start, :stop) => TimeSpan))
        @test nrow(df_result_) == nrow(df_result)
    end

    @testset "gropuby_interval_join" begin
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
        df_combined = combine(groupby_interval_join(df1, quarters, Not([:span, :x]);
                                                    on=:span),
                              :x => mean)
        err = ErrorException("Column span cannot be used for grouping during a call to `split_into_combine`.")
        @test_throws err combine(groupby_interval_join(df1, quarters, All(); on=:span),
                                 :x => mean)
        @test_throws err combine(groupby_interval_join(df1, quarters, Cols(:); on=:span),
                                 :x => mean)

        df2 = DataFrame(; label=rand(('a':'d'), n), sublabel=rand(('k':'n'), n), x=rand(n),
                        span=spans)
        df2_split = combine(groupby_interval_join(df2, quarters,
                                                  Cols(Between(:label, :sublabel),
                                                       :quarter);
                                                  on=:span),
                            :x => mean)
        df2_manual = combine(groupby(interval_join(df2, quarters; on=:span),
                                     Cols(Between(:label, :sublabel), :quarter)),
                             :x => mean)
        @test df2_split.x_mean == df2_manual.x_mean
        @test_throws ErrorException combine(groupby_interval_join(df2, quarters,
                                                                  [:i_dont_exist];
                                                                  on=:span),
                                            :x => mean)
        @test_throws ErrorException combine(groupby_interval_join(df2, quarters, Cols(1:2);
                                                                  on=:span), :x => mean)

        # test lambda columns
        df1_left_right = select(df1, :label, :x,
                                :span => ByRow(x -> (; start=start(x), stop=stop(x))) => AsTable)
        quarters_lr = select(quarters, :quarter,
                             :span => ByRow(x -> (; start=start(x), stop=stop(x))) => AsTable)
        df_combined = combine(groupby_interval_join(df1_left_right, quarters_lr,
                                                    [:quarter, :label];
                                                    on=(:start, :stop) => TimeSpan),
                              :x => mean)
        df_manual_combined = combine(groupby(interval_join(df1_left_right, quarters_lr;
                                                           on=(:start, :stop) => TimeSpan),
                                             [:quarter, :label]), :x => mean)
        @test df_combined.x_mean == df_manual_combined.x_mean
        df_combined = combine(groupby_interval_join(df1, quarters_lr,
                                                    [:quarter, :label];
                                                    on=:span => ((:start, :stop) => TimeSpan)),
                              :x => mean)
        @test df_combined.x_mean == df_manual_combined.x_mean
        df_combined = combine(groupby_interval_join(df1_left_right, quarters,
                                                    [:quarter, :label];
                                                    on=((:start, :stop) => TimeSpan) => :span),
                              :x => mean)
        @test df_combined.x_mean == df_manual_combined.x_mean
    end
end

@testset "Aqua" begin
    Aqua.test_all(DataFrameIntervals;
                  project_extras=true,
                  stale_deps=true,
                  deps_compat=true,
                  project_toml_formatting=true,
                  ambiguities=false)
end
