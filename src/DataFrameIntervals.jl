module DataFrameIntervals

using Intervals, DataFrames, Requires, Dates
export quantile_windows, split_into, split_into_combine

using Infiltrator

#####
##### Support `find_intersection` and `intersect` over `Interval` and `TimeSpan` objects.
#####

function find_intersections_(x::AbstractVector, y::AbstractVector) 
    Intervals.find_intersections(IntervalArray(x), IntervalArray(y))
end
intersect_(x, y) = backto(x, intersect(interval(x), interval(y)))

# IntervalArray is a helper that treats any vector of interval-like objects as an array
# of `Interval` objects.
struct IntervalArray{A, I} <: AbstractVector{I}
    val::A
end
Base.size(x::IntervalArray) = size(x.val)
Base.getindex(x::IntervalArray, i) = interval(x.val[i])
Base.IndexStyle(::Type{<:IntervalArray}) = IndexLinear()

# supprot for `Interval` vectors
IntervalArray(x::AbstractVector{<:Interval}) = x
interval(x::Interval) = x
backto(::Interval, x) = x

# support for `TimeSpan` vectors
function __init__()
    @require TimeSpans = "bb34ddd2-327f-4c4a-bfb0-c98fc494ece1" begin
        using .TimeSpans
        interval(x::TimeSpan) = Interval{Nanosecond, Closed, Open}(x.start, x.stop)
        backto(::TimeSpan, x::Interval{Nanosecond, Closed, Open}) = TimeSpan(first(x), last(x))
        function IntervalArray(x::AbstractVector{<:TimeSpan})
            IntervalArray{typeof(x), Interval{Nanosecond, Closed, Open}}(x)
        end
    end
end

"""
    split_into(left, right; spancol=:span)

    Given two data frames, where rows represent some span of time (specified by `spancol`),
split the intervals in `left` by the intervals in `right`. For example, you could split the
times over which your lights are on in your house (left dataframe) into the preiods of night
and day defined by sunrise and sunset for each day.

In effect, this implements join over left and right, where rows match when the spans
intersect.

In detail: each row of left becomes a set of rows. Each such row is the intersection between
the span of this left row with a span from the right row. That row has the column values for
both left and right. 

Three new columns are defined in the output:

- :left_span - reports span of left joined row
- :right_span - reports span of right joined row
- :span - the intersection of the left and right span
"""
function split_into(left, right; spancol=:span)
    regions = find_intersections_(view(right, :, spancol), view(left, :, spancol))
    left_side, right_side = split(regions, left, right)
    joined = hcat(view(right_side, :, Not(spancol)),
                  view(left_side, :, Not(spancol)))
    spans_for_split!(joined, view(right_side, :, spancol), view(left_side, :, spancol))
    return joined
end

function split(regions, left, right)
    ixs = map(enumerate(regions)) do (right_i, left_ixs)
        return (fill(right_i, length(left_ixs)), left_ixs)
    end
    left_side = view(left, mapreduce(last, vcat, ixs), :)
    right_side = view(right, mapreduce(first, vcat, ixs), :)
    return left_side, right_side
end

function spans_for_split!(df, left_span, right_span)
    df[!, :left_span] = left_span
    df[!, :right_span] = right_span
    transform!(df, [:left_span, :right_span] => ByRow(intersect_) => :span)
    return df
end

# helper for `split_into_combine`
const PairLike = Union{AbstractVector{<:Pair}, <:Pair}
combine_view(df::AbstractDataFrame, pairs::PairLike...) = (span, indices) -> combine_view(span, indices, df, pairs...)
function combine_view(span::AbstractVector, indices::AbstractVector, df::AbstractDataFrame, pairs::PairLike...)
    df, span = split(indices, df, span)
    df = spans_for_split!(copy(df), df.span, vec(span))
    combine(df, pairs...)
end

"""
    split_into_combine(left, right, groups, pairs...; spancol=:span)

    Equivalent to, but less resource intensive than 
`combine(groupby(split_into(left, right), groups), pairs...)`. The one caveat is that
the only column from `right` that `pairs` can reference is `:right_span`.
"""
function split_into_combine(left, right, groups, pairs...; spancol=:span, kwds...)
    regions = find_intersections_(view(right, :, spancol), view(left, :, spancol))
    right = insertcols!(DataFrame(right, copycols=false), :left_index => regions)
    grouped = groupby(right, groups)
    return combine(grouped,
                   [spancol, :left_index] => combine_view(left, pairs...) => AsTable;
                   kwds...)
end

label_helper(x::Symbol) = x
value_helper(::Symbol, n) = 1:n
label_helper(x::Pair) = first(x)
value_helper(x::Pair, _) = last(x)


function intervals(steps, el)
    return map(steps[1:end-1], steps[2:end]) do start, stop
        return backto(el, Interval{eltype(steps), Closed, Open}(start, stop))
    end
end
toval(x::TimePeriod) = float(Dates.value(convert(Nanosecond, x)))
toperiod(x::Real) = Nanosecond(round(Int, x, RoundDown))
range_(a::TimePeriod, b::TimePeriod; length) = map(toperiod, range(toval(a), toval(b); length))
range_(a, b; length) = range(a, b; length)

"""
    quantile_windows(n, span; spancol=:span, label=:count => 1:n, min_duration = 0.75*Intervals.span(span)/n)

Generate a data frame with `n` rows that divide the interval `span` into equally spaced
intervals. The output is a DataFrame with a `:span` column and a column of name `label` with
the index for the span (== 1:n). The label argument can also be a pair in which case it
should be a symbol paired with an iterable of `n` items to assign as the value of the given
column.

The value `span` can also be a dataframe, in which case quantiles that cover the entire
range of time spans in the dataframe are used.

The output is useful as the right argument to `split_into` and `split_into_combine`.
"""
function quantile_windows(n, span_; spancol=:span, label=:index, min_duration=nothing)
    ismissing(span_) && return missing

    span = interval(span_)
    splits = intervals(range_(first(span), last(span); length=n+1), span_)
    min_duration = if isnothing(min_duration) 
        toperiod(0.75*toval(Intervals.span(interval(first(splits)))))
    else
        min_duration
    end
    df = DataFrame(;(spancol => splits, label_helper(label) => value_helper(label, n))...)
    return df
end
function quantile_windows(n, span::DataFrame; spancol=:span, kwds...) 
    return quantile_windows(n, dfspan(span, spancol); spancol, kwds...)
end

function dfspan(df, spancol) 
    if nrow(df) == 0
        return missing
    else
        return backto(first(df[!, spancol]), superset(IntervalArray(df[!, spancol])))
    end
end

end # module