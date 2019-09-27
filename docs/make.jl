using Documenter, RecordStores

makedocs(;
    modules=[RecordStores],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/tkf/RecordStores.jl/blob/{commit}{path}#L{line}",
    sitename="RecordStores.jl",
    authors="Takafumi Arakaki <aka.tkf@gmail.com>",
    assets=String[],
)

deploydocs(;
    repo="github.com/tkf/RecordStores.jl",
)
