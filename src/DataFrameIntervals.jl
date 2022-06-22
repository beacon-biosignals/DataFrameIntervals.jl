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

# IntervalArray is a helper that treats any vector of interval-like objects as an array of
# `Interval` objects. For now this includes only `TimeSpans` (though there are several other
# pacakges that could be supported in theory, e.g. the interval object from AxisArrays)
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

onleft(x) = x
onright(x) = x
onleft(x::Pair) = first(x)
onright(x::Pair) = last(x)

"""
    interval_join(left, right; on, renamecols=identity => identity, 
                  renameon=:_left => :_right, makeunique=false)

Join two dataframes based on the intervals they represent (denoted by the `on` column);
these are typically intervals of time. The join includes one row for every pair of rows in
`left` and `right` whose intervals overlap (i.e. `!isempty(intersect(left.on, right.on))`).

- `on`: The column name to join left and right on. If the column on which left and right
  will be joined have different names, then a left=>right pair can be passed. on is a
  required argument. The value of the on in the output data frame is the intersection of the
  left and right interval.

- `makeunique`: if false (the default), an error will be raised if duplicate names are found
  in columns not joined on; if true, duplicate names will be suffixed with _i (i starting at
  1 for the first duplicate).

- `renamecols`: a Pair specifying how columns of left and right data frames should be
  renamed in the resulting data frame. Each element of the pair can be a string or a Symbol
  can be passed in which case it is appended to the original column name; alternatively a
  function can be passed in which case it is applied to each column name, which is passed to
  it as a String. Note that renamecols does not affect on columns.

- `renameon`: a Pair specifying how the left and right data frame `on` column is renamed and
   stored in the resulting data frame, following the same format as `renamecols`.

"""
function interval_innerjoin(left, right; on, renamecols=identity => identity, 
                            renameon=:_left => :_right, makeunique=false)
    regions = find_intersections_(view(right, :, onright(on)), view(left, :, onleft(on)))
    if !(on isa Symbol || on isa AbstractString)
        error("Interval joins support only one `on` column; iterables are not allowed.")
    end

    left_side, right_side = join_indices(regions, left, right)
    rename!(left, (renamer(n, onleft(renamecols), onleft(on), onleft(renameon)) 
                 for n in names(left))...)
    rename!(right, (renamer(n, onright(renamecols), onright(on), onright(renameon)) 
                 for n in names(right))...)
    # TODO: we need the renamed on column
    if string(onleft(on)) ∈ names(left) || string(onleft(on)) ∈ names(right)
        error("`interval_innerjoin` requires that you give a new name to the `on` column using 
               `renameon`.")
    end
    joined = hcat(right_side, left_side; makeunique)
    transform!(df, [onleft(on), onright(on)] => ByRow(intersect_) => onleft(on))
    add_ons!(joined, on, view(left_side, :, on), view(right_side, :, on), renameon)
    return joined
end
function renamer(n, renamecols, on, renameon)
    return n == string(on) ? n => renamer(n, renameon) : n => renamer(n, renamecols)
end
renamer(col, suffix::Union{Symbol, AbstractString}) = string(col, suffix)
renamer(col, fn) = fn(col)
    
function join_indices(regions, left, right)
    ixs = map(enumerate(regions)) do (right_i, left_ixs)
        return (fill(right_i, length(left_ixs)), left_ixs)
    end
    left_side = view(left, mapreduce(last, vcat, ixs), :)
    right_side = view(right, mapreduce(first, vcat, ixs), :)
    return left_side, right_side
end

# helpers to handle grouping DataFrames
struct Invalid
    name::String
end
Base.string(x::Invalid) = x.name
spancol_error(spancol) = error("Column $spancol cannot be used for grouping during a call to `split_into_combine`.")
function check_spancol(spancol, names)
    string(spancol) ∈ names && spancol_error(spancol)
    return names
end

# `find_valid`: returns a list of all columns from a `DataFrames` column selector that
# are valid when making a call to `split_into_combine`. This has to check that `spancol` is
# not included (since we cannot group by the time span column we're using to compute joins
# Any explicitly specified columns that are not valid are returned as `Invalid` objects.
function find_valid(on, df, col::Union{<:Integer, <:AbstractRange{<:Integer}, <:AbstractVector{<:Integer}})
    error("Cannot use index or boolean as grouping variable when using `split_into_combine`")
end
function find_valid(on, df, col::Union{<:AbstractString, Symbol}) 
    col = string(col)
    return col ∈ names(df) ? check_on(on, Union{String, Invalid}[col]) : Union{String, Invalid}[Invalid(col)]
end
function find_valid(on, df, cols::Not)
    valids = in.(string.(cols.skip), Ref(names(df)))
    check_on(on, names(df, Not(cols.skip[valids])))
end
function find_valid(on, df, cols::Not{<:Union{Symbol, <:AbstractString}})
    if in(string(cols.skip), names(df))
        check_on(on, names(df, cols))
    else
        check_on(on, names(df))
    end
end
find_valid(on, df, cols::All) = on_error(on)
find_valid(on, df, cols::Colon) = on_error(on)
function find_valid(on, df, cols::Cols{<:Tuple{<:Function}})
    check_on(on, names(df, cols))
end
function find_valid(on, df, cols::Cols)
    check_on(on, union(find_valid.(on, Ref(df), cols.cols)...))
end
find_valid(on, df, cols::Regex) = check_on(on, names(df, cols))
function find_valid(on, df, cols::Between)
    first_last = [find_valid(on, df, cols.first); find_valid(on, df, cols.last)]
    if all(x -> x isa String, first_last)
        check_on(on, names(df, cols))
    else
        return filter(x -> x isa Invalid, first_last)
    end
end
find_valid(on, df, cols) = mapreduce(c -> find_valid(on, df, c), vcat, cols)

# helper for `split_into_combine`
const PairLike = Union{AbstractVector{<:Pair}, <:Pair}
combine_view(df, groups, pairs...) = (span, indices) -> _combine_view(span, indices, df, groups, pairs...)
function _combine_view(span, indices, df, groups, pairs...)
    df, span = split(indices, df, span)
    df = spans_for_split!(copy(df), df.span, vec(span))
    return combine(groupby(df, groups), pairs...)
end

"""
    combine_interval_join(left, right, groups, pairs...; on=:span)

    Equivalent to, but less resource intensive than 
`combine(groupby(split_into(left, right), groups), pairs...)`. The one caveat is that
the only column from `right` that `pairs` can reference is the `on` column.
"""
function combine_interval_join(left, right, groups, pairs...; on=:span, kwds...)
    regions = find_intersections_(view(right, :, on), view(left, :, on))
    right = insertcols!(DataFrame(right, copycols=false), :left_index => regions)
    
    # the groupings passed apply to both left and right data frames but we need to groupby
    # *befure* we combine them so we need our own methods to figure out which columns belong
    # to which dataframe; this is all complicated by the fact that columns 
    # can be specified in a variety of different ways: e.g. Not(:span).

    right_groups = find_valid(on, right, groups)
    left_groups = find_valid(on, left, groups)

    right_cols = filter(x -> x isa String, right_groups)
    right_invalid = filter(x -> x isa Invalid, right_groups)
    left_cols = filter(x -> x isa String, left_groups)
    left_invalid = filter(x -> x isa Invalid, left_groups)
    
    invalid = intersect(right_invalid, left_invalid)
    if !isempty(invalid)
        error("Columns do not exist: "*join(string.(invalid), ", ", " and "))
    end
    grouped = groupby(right, right_cols)
    
    result = combine(grouped,
                     [on, :left_index] => combine_view(left, left_cols, pairs...) => AsTable;
                     kwds...)
    if :left_index ∈ propertynames(result)
        return select(result, Not(:left_index))
    else
        return result
    end
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
