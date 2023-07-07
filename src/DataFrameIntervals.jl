module DataFrameIntervals

using Intervals, DataFrames, Requires, Dates
export quantile_windows, interval_join, groupby_interval_join

# represents a lazy application of fn.(vec1, vec2, ...), useful in cases where
# fn is cheap and we don't mind recomputing it for each call to `getindex`
struct FnVector{T,F,A} <: AbstractVector{T}
    fn::F
    args::A
end
Base.size(x::FnVector) = size(x.args[1])
function lazy(fn::Base.Callable, args::AbstractVector...) 
    sizes = size.(args)
    all(==(first(sizes)), sizes) || throw(ArgumentError("Vectors must all have the same size."))
    isempty(args) && throw(ArgumentError("Must use non-empty vectors"))
    T = typeof(fn(getindex.(args, 1)...))
    return FnVector{T,typeof(fn),typeof(args)}(fn, args)
end
Base.@propagate_inbounds Base.getindex(x::FnVector, i::Int) = x.fn(getindex.(x.args, i)...)

#####
##### Support `find_intersection` and `intersect` over `Interval` and `TimeSpan` objects.
#####

function find_intersections_(x::AbstractVector, y::AbstractVector)
    return Intervals.find_intersections(lazy(interval, x), lazy(interval, y))
end
function intersect_(x, y)
    ismissing(x) && return missing
    ismissing(y) && return missing
    return backto(x, intersect(interval(x), interval(y)))
end

# support for `Interval` vectors
interval(x::Interval) = x
interval(x::Missing) = missing
backto(::Interval, x) = x
backto(::Missing, x) = missing

# bypass lazy array if we know it is == identity
lazy(::typeof(interval), x::AbstractVector{<:Interval}) = x
lazy(::typeof(interval), x::AbstractVector{<:Union{Missing, Interval}}) = x

# support for `NamedTuple` vectors
const IntervalTuple = Union{NamedTuple{(:start, :stop)},NamedTuple{(:stop, :start)}}
interval_type(x::Type{<:T}) where {T<:IntervalTuple} = Union{T.parameters[2].parameters...}
interval_type(x::IntervalTuple) = Union{typeof(x).parameters[2].parameters...}
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
    end
end

forleft(x) = x
forright(x) = x
forleft(x::Pair) = !istransform(x) ? first(x) : x
forright(x::Pair) = !istransform(x) ? last(x) : x

is_col_selector(::AbstractString) = true
is_col_selector(::Symbol) = true
is_col_selector(x::Tuple) = all(x -> x isa Symbol || x isa AbstractString, x)
is_col_selector(x::Pair) = istransform(x)
is_col_selector(x) = false
istransform(x) = false
function istransform(x::Pair)
    return (is_col_selector(first(x)) || 
            (first(x) isa Tuple && all(is_col_selector, first(x)))) && 
           !is_col_selector(last(x))
end

is_valid_on(x) = is_col_selector(x)
is_valid_on(x::Pair) = (is_col_selector(first(x)) && is_col_selector(last(x))) || istransform(x)
function interval_transformer(x)
    cols = if first(x) isa Tuple
        Cols(first(x)...)
    else
        first(x)
    end
    fn, label = last(x), Symbol(gensym("span"))
    if first(x) isa Tuple
        return cols => ((args...) -> lazy(fn, args...)) => label
    else
        return cols => (a -> lazy(fn, a)) => label
    end
end

input_columns(x::AbstractString) = (x,)
input_columns(x::Symbol) = (x,)
function input_columns(x::Pair) 
    if first(x) isa Tuple
        return first(x)
    else
        return (x,)
    end
end

function renamer(n, renamecols, oncols, renameon)
    return n in string.(oncols) ? n => rename_col(n, renameon) : n => rename_col(n, renamecols)
end
rename_col(col::Union{Symbol,AbstractString}, suffix::Union{Symbol,AbstractString}) = string(col, suffix)
rename_col(col::Union{Symbol,AbstractString}, fn) = fn(col)

function setup_column_names!(left, right; on, renamecols=identity => identity,
                             renameon=:_left => :_right, outcol=:span)
    if !is_valid_on(on)
        error("Unexpected value for `on` column: $on. Refer to `interval_join` ",
              "documentation for supported values.")
    end

    left_out = if istransform(forleft(on))
        trans = interval_transformer(forleft(on))
        transform!(left, trans)
        last(last(trans))
    else
        forleft(on)
    end
    right_out = if istransform(forright(on))
        trans = interval_transformer(forright(on))
        transform!(right, trans)
        last(last(trans))
    else
        forright(on)
    end
    
    left_ins = input_columns(forleft(on))
    right_ins = input_columns(forright(on))
    left_cols = (left_out, left_ins...)
    right_cols = (right_out, right_ins...)
    (left_on, left_in_rename...) = rename_col.(left_cols, Ref(forleft(renameon)))
    (right_on, right_in_rename...) = rename_col.(right_cols, Ref(forright(renameon)))
    rename!(left,
            (renamer(n, forleft(renamecols), left_cols, forleft(renameon))
             for n in names(left))...)
    rename!(right,
            (renamer(n, forright(renamecols), right_cols, forright(renameon))
             for n in names(right))...)

    if any(l -> string(l) == string(outcol), left_in_rename)
        error("Interval join failed: left dataframe's `on` column(s) has the final name(s) ",
              "`$(join(left_ins, ", ", " and "))` which clashes with joined dataframe's `on` column name ",
              "`$outcol`. Make sure `renameon` is set properly.")
    end
    if any(r -> string(r) == string(outcol), right_in_rename)
        error("Interval join failed: right dataframe's `on` column(s) has the final name(s) ",
              "`$(join(right_ins, ", ", " and "))` which clashes with joined dataframe's `on` column name ",
              "`$outcol`. Make sure `renameon` is set properly.")
    end
    remove_left = if left_on != first(left_in_rename)
        (left_on,)
    else
        ()
    end
    remove_right = if right_on != first(right_in_rename)
        (right_on,)
    else
        ()
    end

    return (; left_on, right_on, joined_on=outcol, removecols = (remove_left..., remove_right...), left, right)
end

"""
    interval_join(left, right; on, renamecols=identity => identity, 
                  renameon=:_left => :_right, makeunique=false, keepleft=false,
                  keepright=false, outcol=:span)

Join two dataframes based on the intervals they represent (denoted by the `on` column);
these are typically intervals of time. By default, the join includes one row for every
pairing of rows in `left` and `right` whose intervals overlap (i.e. `!isdisjoint(left.on,
right.on))`).

- `on`: The column name(s) to join left and right on. Can take one of several forms.
    1. column name: each dataframe should include this column, and the output will also
      include it. It should be an `Interval`, `TimeSpan` or `NamedTuple`.
    2. left-right column-name pair (`left => right`): the left dataframe will be joined on
    the column name in `left` and the right on column name in `right`, the result will
    include the `left` name.
    3. colmun-lambda-result pairs: (`(:col, ...) => fn`) both data frames will transform the
    given columns by `fn`. It should take as many positional arguments as their are columns
    and should return an `Interval`, `TimeSpan` or `NamedTuple` type. 
    4. a left-right column pair: (`(:col => fn) => right`) some combination of 2 and 3
    (requires parenthesis to diambiguate). 

- `makeunique`: if false (the default), an error will be raised if duplicate names are found
  in columns not joined on; if true, duplicate names will be suffixed with _i (i starting at
  1 for the first duplicate).

- `renamecols`: a Pair specifying how columns of left and right data frames should be
  renamed in the resulting data frame. Each element of the pair can be a string or a Symbol,
  in which case it is appended to the original column name; alternatively a function can be
  passed in which case it is applied to each column name, which is passed to it as a String.
  Note that renamecols does not affect any of the `on` columns.

- `renameon`: a Pair specifying how the left and right data frame `on` column(s) is (are)
   renamed and stored in the resulting data frame, following the same format as
   `renamecols`.

- `keepleft`: if true, keep rows in left that don't match rows in right (ala `leftjoin`)

- `keepright`: if true, keep rows in right that don't match rows in left (ala `rightjoin`)

- `outcol`: the name of the column that reports the intersection between the left
  and right interval

"""
function interval_join(left, right; makeunique=false, keepleft=false, keepright=false,
                       kwds...)
    left = DataFrame(left; copycols=false)
    right = DataFrame(right; copycols=false)
    (left_on, right_on, joined_on, removecols) = setup_column_names!(left, right; kwds...)
    right_missing = any(ismissing, view(right, :, right_on))
    left_missing = any(ismissing, view(left, :, left_on))
    if right_missing || left_missing
        throw(ArgumentError("There are missing values in the " *
                            "$(left_missing ? "left" : "right") table of `interval_join`."))
    end
    # `isempty` checks will be uncessary in future versions of Intervals.jl
    # (c.f. https://github.com/invenia/Intervals.jl/pull/201)
    regions = isempty(right) || isempty(left) ? Vector{Int}[] :
              find_intersections_(view(right, :, right_on), view(left, :, left_on))

    # perform the join
    joined = join_indices(regions, left, right; keepleft, keepright, makeunique)
    transform!(joined, Symbol.([left_on, right_on]) => ByRow(intersect_) => joined_on)
    return select!(joined, Not(Cols(joined_on, removecols...)), joined_on)
end

function join_indices(regions, left, right; keepleft=false, keepright=false, makeunique)
    isempty(regions) && return vcat(left[1:0, :], right[1:0, :]; cols=:union)
    from_both = map(enumerate(regions)) do (right_i, left_ixs)
        return (fill(right_i, length(left_ixs)), left_ixs)
    end

    from_left = if keepleft
        setdiff(axes(left, 1), reduce(union, regions))
    else
        Int[]
    end

    from_right = if keepright
        findall(isempty, regions)
    else
        Int[]
    end

    left_side = view(left, mapreduce(last, vcat, from_both), :)
    right_side = view(right, mapreduce(first, vcat, from_both), :)
    joined = hcat(left_side, right_side; makeunique)
    return reduce(vcat, (joined, view(left, from_left, :), view(right, from_right, :));
                  cols=:union)
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

struct GroupedIntervalJoin{R,LG,LD,N}
    right_grouped::R
    left_groups::LG
    left_df::LD
    makeunique::Bool
    left_index::Symbol
    left_on::Symbol
    right_on::Symbol
    joined_on::Symbol
    removecols::NTuple{N, Symbol}
end

"""
    groupby_interval_join(left, right, groups; on, renamecols=identity => identity, 
                          renameon=:_left => :_right, makeunique=false, outcol=:span)

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
    (left_on, right_on, joined_on, removecols) = setup_column_names!(left, right; on, kwds...)

    # compute interval intersections
    left_index = gensym(:__left_index__)
    regions = find_intersections_(view(right, :, right_on), view(left, :, left_on))
    right = insertcols!(right, left_index => regions)

    # a lazy instantiation of the joined dataframe
    return GroupedIntervalJoin(groupby(right, right_cols), left_cols, left, makeunique,
                               Symbol(left_index), Symbol(left_on),
                               Symbol(right_on), Symbol(joined_on), Symbol.(removecols))
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
    joined = join_indices(right_df[!, grouped.left_index], left_df, right_df;
                          grouped.makeunique)
    df = transform!(joined,
                    [grouped.left_on, grouped.right_on] => ByRow(intersect_) => grouped.joined_on)
    select!(df, Not(Cols(grouped.joined_on, grouped.removecols...)), grouped.joined_on)
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
        return backto(first(df[!, spancol]),
                      superset(IntervalSet(lazy(interval, df[!, spancol]))))
    end
end

end # module
