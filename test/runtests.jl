using RecordStores
using Test

@testset "RecordStores.jl" begin
    records = [
        Dict(:a => 1, :b => [1, 2, 3]),
        Dict(:c => [1.0, 2.0, 3im]),
    ]

    mktemp() do path, io
        store = recordstore(path)
        open(store, "w") do w
            for r in records
                write(w, r)
            end
        end

        @test read(store) == records
    end
end
