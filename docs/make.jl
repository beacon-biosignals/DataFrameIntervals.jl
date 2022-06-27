using DataFrameIntervals
using Documenter

DocMeta.setdocmeta!(DataFrameIntervals, :DocTestSetup, :(using DataFrameIntervals);
                    recursive=true)

makedocs(;
         modules=[DataFrameIntervals],
         repo="https://github.com/beacon-biosignals/DataFrameIntervals.jl/blob/{commit}{path}#{line}",
         sitename="DataFrameIntervals.jl",
         format=Documenter.HTML(;
                                prettyurls=get(ENV, "CI", "false") == "true",
                                assets=String[]),
         pages=["Home" => "index.md"])
