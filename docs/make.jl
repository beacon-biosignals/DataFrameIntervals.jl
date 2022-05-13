using DataFrameIntervals
using Documenter

DocMeta.setdocmeta!(DataFrameIntervals, :DocTestSetup, :(using DataFrameIntervals); recursive=true)

makedocs(;
    modules=[DataFrameIntervals],
    authors="David F Little <david.frank.little@gmail.com> and contributors",
    repo="https://github.com/haberdashpi/DataFrameIntervals.jl/blob/{commit}{path}#{line}",
    sitename="DataFrameIntervals.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)
