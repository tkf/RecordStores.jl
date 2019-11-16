using RecordStores
using Test

@testset "RecordStores.jl" begin
    records = [
        Dict(:a => 1, :b => [1, 2, 3]),
        Dict(:c => [1.0, 2.0, 3im]),
    ]

    @testset for archiver in [:zip, :dir]
        mktempdir() do dir
            store = recordstore(joinpath(dir, "store"); archiver = archiver)
            open(store, "w") do w
                for r in records
                    write(w, r)
                end
            end

            @test read(store) == records
        end
    end
end

@testset "guess_archiver" begin
    if Sys.isunix()
        @test recordstore("/store.zip").writer == RecordStores.ZipFile.Writer
        @test recordstore("/store/").writer == RecordStores.DirWriter
    end
end
