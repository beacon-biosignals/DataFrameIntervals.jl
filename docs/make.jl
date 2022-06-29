using DataFrameIntervals
using Documenter

makedocs(; modules=[DataFrameIntervals],
         authors="Beacon Biosignals, Inc.",
         repo="https://github.com/beacon-biosignals/DataFrameIntervals.jl/blob/{commit}{path}#{line}",
         sitename="DataFrameIntervals.jl",
         format=Documenter.HTML(; prettyurls=get(ENV, "CI", "false") == "true",
                                # will be re-directed as needbe for a private repo
                                canonical="https://beacon-biosignals.github.io/DataFrameIntervals.jl",
                                assets=String[]),
         pages=["Home" => "index.md"])

deploydocs(; repo="github.com/beacon-biosignals/DataFrameIntervals.jl",
           devbranch="main",
           push_preview=true)
