module DataFrameIntervals

using Intervals, DataFrames, Requires, Dates
export quantile_windows, interval_join, groupby_interval_join

#####
##### Support `find_intersection` and `intersect` over `Interval` and `TimeSpan` objects.
#####

function find_intersections_(x::AbstractVector, y::AbstractVector)
    return Intervals.find_intersections(IntervalArray(x), IntervalArray(y))
end
intersect_(x, y) = backto(x, intersect(interval(x), interval(y)))

# IntervalArray is a helper that treats any vector of interval-like objects as an array of
# `Interval` objects. For now this includes only `TimeSpans` and `NamedTuple` objects with 
# a `start` and `stop` field
struct IntervalArray{A,I} <: AbstractVector{I}
    val::A
end
Base.size(x::IntervalArray) = size(x.val)
Base.getindex(x::IntervalArray, i) = interval(x.val[i])
Base.IndexStyle(::Type{<:IntervalArray}) = IndexLinear()

# support for `Interval` vectors
IntervalArray(x::AbstractVector{<:Interval}) = x
interval(x::Interval) = x
backto(::Interval, x) = x

# support for `NamedTuple` vectors
const IntervalTuple = Union{NamedTuple{(:start, :stop)},NamedTuple{(:stop, :start)}}
interval_type(x::Type{<:T}) where {T<:IntervalTuple} = Union{T.parameters[2].parameters...}
interval_type(x::IntervalTuple) = Union{typeof(x).parameters[2].parameters...}
function IntervalArray(x::AbstractVector{<:IntervalTuple})
    return IntervalArray{typeof(x),Interval{interval_type(eltype(x)),Closed,Open}}(x)
end
interval(x::IntervalTuple) = Interval{interval_type(x),Closed,Open}(x.start, x.stop)
backto(::NamedTuple{(:start, :stop)}, x::Interval) = (; start=first(x), stop=last(x))
backto(::NamedTuple{(:stop, :start)}, x::Interval) = (; stop=last(x), start=first(x))

# support for `TimeSpan` vectors
function __init__()
    @require TimeSpans = "bb34ddd2-327f-4c4a-bfb0-c98fc494ece1" begin
        using .TimeSpans
        interval(x::TimeSpan) = Interval{Nanosecond,Closed,Open}(x.start, x.stop)
        function backto(::TimeSpan, x::Interval{Nanosecond,Closed,Open})
            return TimeSpan(first(x), last(x))
        end
        function IntervalArray(x::AbstractVector{<:TimeSpan})
            return IntervalArray{typeof(x),Interval{Nanosecond,Closed,Open}}(x)
        end
    end
end

forleft(x) = x
forright(x) = x
forleft(x::Pair) = first(x)
forright(x::Pair) = last(x)

function setup_column_names!(left, right; on, renamecols=identity => identity,
                             renameon=:_left => :_right)
    if !(on isa Union{Symbol, AbstractString, Pair{Symbol, Symbol}, 
                      Pair{<:AbstractString, <:AbstractString}})
        error("Interval joins support only one `on` column; iterables are not allowed.")
    end

    left_on = renamer(forleft(on), forleft(renameon))
    right_on = renamer(forright(on), forright(renameon))
    joined_on = forleft(on)
    rename!(left,
            (renamer(n, forleft(renamecols), forleft(on), forleft(renameon))
             for n in names(left))...)
    rename!(right,
            (renamer(n, forright(renamecols), forright(on), forright(renameon))
             for n in names(right))...)
    if string(left_on) == string(joined_on)
        error("Interval join failed: left dataframe's `on` column has the final name ",
              "`$left_on` which clashes with joined dataframe's `on` column name ",
              "`$joined_on`. Make sure `renameon` is set properly.")
    end
    if string(right_on) == string(joined_on)
        error("Interval join failed: right dataframe's `on` column has the final name ",
              "`$right_on` which clashes with joined dataframe's `on` column name ",
              "`$joined_on`. Make sure `renameon` is set properly.")
    end

    return (; left_on, right_on, joined_on, left, right)
end

"""
    interval_join(left, right; on, renamecols=identity => identity, 
                  renameon=:_left => :_right, makeunique=false)

Join two dataframes based on the intervals they represent (denoted by the `on` column);
these are typically intervals of time. The join includes one row for every pairing of rows
in `left` and `right` whose intervals overlap (i.e. `!isempty(intersect(left.on,
right.on))`).

- `on`: The column name to join left and right on. If the column on which left and right
  will be joined have different names, then a left=>right pair can be passed. on is a
  required argument. The value of the on column in the output data frame is the intersection
  of the left and right interval. `on` can be one of three different types of objects:
  an `Interval`, a `TimeSpan` or a `NamedTuple` with a `start` and a `stop` field.

- `makeunique`: if false (the default), an error will be raised if duplicate names are found
  in columns not joined on; if true, duplicate names will be suffixed with _i (i starting at
  1 for the first duplicate).

- `renamecols`: a Pair specifying how columns of left and right data frames should be
  renamed in the resulting data frame. Each element of the pair can be a string or a Symbol
  can be passed in which case it is appended to the original column name; alternatively a
  function can be passed in which case it is applied to each column name, which is passed to
  it as a String. Note that renamecols does not affect any of the `on` columns.

- `renameon`: a Pair specifying how the left and right data frame `on` column is renamed and
   stored in the resulting data frame, following the same format as `renamecols`.

"""
function interval_join(left, right; makeunique=false, kwds...)
    left = DataFrame(left; copycols=false)
    right = DataFrame(right; copycols=false)
    (; left_on, right_on, joined_on) = setup_column_names!(left, right; kwds...)
    regions = find_intersections_(view(right, :, right_on), view(left, :, left_on))

    # perform the join
    left_side, right_side = join_indices(regions, left, right)
    joined = hcat(right_side, left_side; makeunique)
    transform!(joined, [left_on, right_on] => ByRow(intersect_) => joined_on)
    return joined
end
function renamer(n, renamecols, on, renameon)
    return n == string(on) ? n => renamer(n, renameon) : n => renamer(n, renamecols)
end
renamer(col, suffix::Union{Symbol,AbstractString}) = string(col, suffix)
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
function oncol_error(on)
    return error("Column $on cannot be used for grouping during a call to `split_into_combine`.")
end
function check_oncol(on, names)
    string(on) ∈ names && oncol_error(on)
    return names
end

# `find_valid`: given a DataFrame column selector return an array of strings and `Invalid`
# objects. The strings represent all columns present in the dataframe that would be selected
# by the given selector. Any `Invalid` values are columns the selector requestred that were
# not actually present in the dataframe. 
function find_valid(on, df,
                    col::Union{<:Integer,<:AbstractRange{<:Integer},
                               <:AbstractVector{<:Integer}})
    return error("Cannot use index or boolean as grouping variable when using `split_into_combine`")
end
function find_valid(on, df, col::Union{<:AbstractString,Symbol})
    col = string(col)
    return col ∈ names(df) ? check_oncol(on, Union{String,Invalid}[col]) :
           Union{String,Invalid}[Invalid(col)]
end
function find_valid(on, df, cols::Not)
    valids = in.(string.(cols.skip), Ref(names(df)))
    return check_oncol(on, names(df, Not(cols.skip[valids])))
end
function find_valid(on, df, cols::Not{<:Union{Symbol,<:AbstractString}})
    if in(string(cols.skip), names(df))
        check_oncol(on, names(df, cols))
    else
        check_oncol(on, names(df))
    end
end
find_valid(on, df, cols::All) = oncol_error(on)
find_valid(on, df, cols::Colon) = oncol_error(on)
function find_valid(on, df, cols::Cols{<:Tuple{<:Function}})
    return check_oncol(on, names(df, cols))
end
function find_valid(on, df, cols::Cols)
    return check_oncol(on, union(find_valid.(on, Ref(df), cols.cols)...))
end
find_valid(on, df, cols::Regex) = check_oncol(on, names(df, cols))
function find_valid(on, df, cols::Between)
    first_last = [find_valid(on, df, cols.first); find_valid(on, df, cols.last)]
    if all(x -> x isa String, first_last)
        check_oncol(on, names(df, cols))
    else
        return filter(x -> x isa Invalid, first_last)
    end
end
find_valid(on, df, cols) = mapreduce(c -> find_valid(on, df, c), vcat, cols)

# helper for `split_into_combine`

struct GroupedIntervalJoin{R,LG,LD}
    right_grouped::R
    left_groups::LG
    left_df::LD
    makeunique::Bool
    left_index::Symbol
    left_on::Symbol
    right_on::Symbol
    joined_on::Symbol
end

"""
    groupby_interval_join(left, right, groups; on, renamecols=identity => identity, 
                          renameon=:_left => :_right, makeunique=false)

    Similar to, but less resource intensive than 
`groupby(interval_join(left, right), groups)`. You can iterate over the groups or call
`combine` on said groups. Note however that the returned object is not a `GroupedDataFrame`
and only supports these two operations.

See also [`interval_join`](@ref)
"""
function groupby_interval_join(left, right, groups; on, makeunique=false, kwds...)
    # split column groupings into `left` columns and `right` columns
    right_groups = find_valid(forright(on), right, groups)
    left_groups = find_valid(forleft(on), left, groups)

    right_cols = filter(x -> x isa String, right_groups)
    right_invalid = filter(x -> x isa Invalid, right_groups)
    left_cols = filter(x -> x isa String, left_groups)
    left_invalid = filter(x -> x isa Invalid, left_groups)
    invalid = intersect(right_invalid, left_invalid)
    if !isempty(invalid)
        error("Columns do not exist: " * join(string.(invalid), ", ", " and "))
    end

    # setup column names
    left = DataFrame(left; copycols=false)
    right = DataFrame(right; copycols=false)
    (; left_on, right_on, joined_on) = setup_column_names!(left, right; on, kwds...)

    # compute interval intersections
    left_index = gensym(:__left_index__)
    regions = find_intersections_(view(right, :, right_on), view(left, :, left_on))
    right = insertcols!(right, left_index => regions)

    # a lazy instantiation of the joined dataframe
    return GroupedIntervalJoin(groupby(right, right_cols), left_cols, left, makeunique,
                               Symbol(left_index), Symbol(left_on), Symbol(right_on),
                               Symbol(joined_on))
end

function Base.iterate(grouped::GroupedIntervalJoin)
    mapped = Iterators.map(grouped.right_grouped) do gdf
        return groupby(select!(joingroup(gdf, grouped), Not(grouped.left_index)),
                       grouped.left_groups)
    end
    iterable = Iterators.flatten(mapped)

    result = iterate(iterable)
    isnothing(result) && return nothing
    item, state = result
    return item, (iterable, state)
end
function Base.iterate(::GroupedIntervalJoin, (iterable, state))
    result = iterate(iterable, state)
    isnothing(result) && return nothing
    item, state = result
    return item, (iterable, state)
end

function joingroup(right_df, grouped)
    left_df = grouped.left_df
    left_side, right_side = join_indices(right_df[!, grouped.left_index], left_df, right_df)
    joined = hcat(right_side, left_side; grouped.makeunique)
    return transform!(joined,
                      [grouped.left_on, grouped.right_on] => ByRow(intersect_) => grouped.joined_on)
end

function DataFrames.combine(grouped::GroupedIntervalJoin, pairs...; kwargs...)
    helper = x -> combine(groupby(joingroup(DataFrame(x), grouped), grouped.left_groups),
                          pairs...; kwargs...)
    result = combine(grouped.right_grouped, AsTable(:) => helper => AsTable; kwargs...)
    if grouped.left_index ∈ propertynames(result)
        return select!(result, Not(grouped.left_index))
    else
        return result
    end
end

label_helper(x::Symbol) = x
value_helper(::Symbol, n) = 1:n
label_helper(x::Pair) = first(x)
value_helper(x::Pair, _) = last(x)

function intervals(steps, el)
    return map(steps[1:(end - 1)], steps[2:end]) do start, stop
        return backto(el, Interval{eltype(steps),Closed,Open}(start, stop))
    end
end
toval(x::TimePeriod) = float(Dates.value(convert(Nanosecond, x)))
asnanoseconds(x::Real) = Nanosecond(round(Int, x, RoundDown))
function range_(a::TimePeriod, b::TimePeriod; length)
    return map(asnanoseconds, range(toval(a), toval(b); length))
end
range_(a, b; length) = range(a, b; length)

"""
    quantile_windows(n, span; spancol=:span, label=:count => 1:n, 
                     min_duration = 0.75*Intervals.span(span)/n)

Generate a data frame with `n` rows that divide the interval `span` into equally spaced
intervals. The output is a DataFrame with a `:span` column and a column of name `label` with
the index for the span (== 1:n). The label argument can also be a pair in which case it
should be a symbol paired with an iterable of `n` items to assign as the value of the given
column.

The value `span` can also be a dataframe, in which case quantiles that cover the entire
range of time spans in the dataframe are used.

The output is useful as the right argument to `interva_join` and `groupby_interval_join`
"""
function quantile_windows(n, span_; spancol=:span, label=:index, min_duration=nothing)
    ismissing(span_) && return missing

    span = interval(span_)
    splits = intervals(range_(first(span), last(span); length=n + 1), span_)
    min_duration = if isnothing(min_duration)
        asnanoseconds(0.75 * toval(Intervals.span(interval(first(splits)))))
    else
        min_duration
    end
    df = DataFrame(; (spancol => splits, label_helper(label) => value_helper(label, n))...)
    return df
end
function quantile_windows(n, span::AbstractDataFrame; spancol=:span, kwds...)
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
