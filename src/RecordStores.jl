module RecordStores

export recordstore

import BSON
import ZipFile
using BangBang: push!!

recordstore(path::AbstractString) = RecordStore(path)

struct RecordStore
    path::String
end

function Base.open(store::RecordStore, mode::AbstractString)
    if mode == "w"
        return RecordWriter(store)
    elseif mode == "r"
        return RecordReader(store)
    else
        throw(ArgumentError("Unsupported mode: $mode"))
    end
end

Base.read(store::RecordStore) = open(read, store, "r")

struct RecordWriter
    path::String
    writer
    counter::typeof(Ref(0))
end

RecordWriter(path::AbstractString) =
    RecordWriter(path, ZipFile.Writer(path), Ref(0))
RecordWriter(store::RecordStore) = RecordWriter(store.path)

Base.close(w::RecordWriter) = close(w.writer)

function Base.write(w::RecordWriter, obj)
    i = w.counter[] += 1
    f = ZipFile.addfile(w.writer, "$i.bson")
    obj = deepcopy(obj)  # https://github.com/MikeInnes/BSON.jl/issues/26
    try
        BSON.bson(f, obj)
    finally
        close(f)  # flush
    end
    return w
end

struct RecordReader
    path::String
    reader
end

RecordReader(path::AbstractString) = RecordReader(path, ZipFile.Reader(path))
RecordReader(store::RecordStore) = RecordReader(store.path)

Base.close(r::RecordReader) = close(r.reader)

function Base.read(r::RecordReader)
    id(x) = parse(Int, chop(x.name; tail=length(".bson")))
    files = sort!(collect(r.reader.files); by=id)
    return mapfoldl(BSON.load, push!!, files; init=Union{}[])
end

end # module