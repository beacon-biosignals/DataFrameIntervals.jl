var documenterSearchIndex = {"docs":
[{"location":"","page":"Home","title":"Home","text":"CurrentModule = DataFrameIntervals","category":"page"},{"location":"#DataFrameIntervals","page":"Home","title":"DataFrameIntervals","text":"","category":"section"},{"location":"","page":"Home","title":"Home","text":"Documentation for DataFrameIntervals.","category":"page"},{"location":"","page":"Home","title":"Home","text":"","category":"page"},{"location":"","page":"Home","title":"Home","text":"Modules = [DataFrameIntervals]","category":"page"},{"location":"#DataFrameIntervals.groupby_interval_join-Tuple{Any, Any, Any}","page":"Home","title":"DataFrameIntervals.groupby_interval_join","text":"groupby_interval_join(left, right, groups; on, renamecols=identity => identity, \n                      renameon=:_left => :_right, makeunique=false, outcol=:span)\n\nSimilar to, but less resource intensive than\n\ngroupby(interval_join(left, right), groups). You can iterate over the groups or call combine on said groups. Note however that the returned object is not a GroupedDataFrame and only supports these two operations.\n\nSee also interval_join\n\n\n\n\n\n","category":"method"},{"location":"#DataFrameIntervals.interval_join-Tuple{Any, Any}","page":"Home","title":"DataFrameIntervals.interval_join","text":"interval_join(left, right; on, renamecols=identity => identity, \n              renameon=:_left => :_right, makeunique=false, keepleft=false,\n              keepright=false, outcol=:span)\n\nJoin two dataframes based on the intervals they represent (denoted by the on column); these are typically intervals of time. By default, the join includes one row for every pairing of rows in left and right whose intervals overlap (i.e. !isdisjoint(left.on, right.on))).\n\non: The column name(s) to join left and right on. Can take one of several forms.\ncolumn name: each dataframe should include this column, and the output will also\ninclude it. It should be an `Interval`, `TimeSpan` or `NamedTuple`.\nleft-right column-name pair (left => right): the left dataframe will be joined on\nthe column name in left and the right on column name in right, the result will   include the left name.\ncolmun-lambda-result pairs: ((:col, ...) => fn) both data frames will transform the\ngiven columns by fn. It should take as many positional arguments as their are columns   and should return an Interval, TimeSpan or NamedTuple type. \na left-right column pair: ((:col => fn) => right) some combination of 2 and 3\n(requires parenthesis to diambiguate). \nmakeunique: if false (the default), an error will be raised if duplicate names are found in columns not joined on; if true, duplicate names will be suffixed with _i (i starting at 1 for the first duplicate).\nrenamecols: a Pair specifying how columns of left and right data frames should be renamed in the resulting data frame. Each element of the pair can be a string or a Symbol, in which case it is appended to the original column name; alternatively a function can be passed in which case it is applied to each column name, which is passed to it as a String. Note that renamecols does not affect any of the on columns.\nrenameon: a Pair specifying how the left and right data frame on column(s) is (are)  renamed and stored in the resulting data frame, following the same format as  renamecols.\nkeepleft: if true, keep rows in left that don't match rows in right (ala leftjoin)\nkeepright: if true, keep rows in right that don't match rows in left (ala rightjoin)\noutcol: the name of the column that reports the intersection between the left and right interval\n\n\n\n\n\n","category":"method"},{"location":"#DataFrameIntervals.quantile_windows-Tuple{Any, Any}","page":"Home","title":"DataFrameIntervals.quantile_windows","text":"quantile_windows(n, span; spancol=:span, label=:count => 1:n, \n                 min_duration = 0.75*Intervals.span(span)/n)\n\nGenerate a data frame with n rows that divide the interval span into equally spaced intervals. The output is a DataFrame with a :span column and a column of name label with the index for the span (== 1:n). The label argument can also be a pair in which case it should be a symbol paired with an iterable of n items to assign as the value of the given column.\n\nThe value span can also be a dataframe, in which case quantiles that cover the entire range of time spans in the dataframe are used.\n\nThe output is useful as the right argument to interva_join and groupby_interval_join\n\n\n\n\n\n","category":"method"}]
}