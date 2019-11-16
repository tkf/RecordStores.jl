module RecordStores

export recordstore

import BSON
import ZipFile
using BangBang: push!!
using Base: open_flags

addfile(writer::ZipFile.Writer, name) = ZipFile.addfile(writer, name)
files(x) = x.files
filename(x) = x.name

struct DirWriter
    path::String

    function DirWriter(path)
        mkdir(path)
        return new(path)
    end
end

addfile(writer::DirWriter, name) = joinpath(writer.path, name)

Base.close(::DirWriter) = nothing
Base.flush(::DirWriter) = nothing

struct DirReader
    path::String
end

files(reader::DirReader) = joinpath.(reader.path, readdir(reader.path))
filename(fullpath::AbstractString) = basename(fullpath)
Base.close(::DirReader) = nothing

const _archivers = (
    zip = (writer = ZipFile.Writer, reader = ZipFile.Reader),
    dir = (writer = DirWriter, reader = DirReader),
)

function guess_archiver(path)
    if endswith(path, ".zip")
        return :zip
    elseif endswith(path, Base.Filesystem.path_separator) || isdir(path)
        return :dir
    end
    error(
        "Cannot guess `archiver` for path `$path`.",
        " Please specify `archiver` keyword argument.",
    )
end

"""
    recordstore(path; archiver) -> store

# Keyword Arguments
- `archiver ∈ (:zip, :dir)`
"""
recordstore(path::AbstractString; archiver::Symbol = guess_archiver(path)) =
    RecordStore(path; pairs(_archivers[archiver])...)

struct RecordStore
    path::String
    writer
    reader
end

RecordStore(path; writer, reader) = RecordStore(path, writer, reader)

function Base.open(store::RecordStore; read=nothing, write=nothing)
    flags = open_flags(read=read, write=write)
    flags.write && return RecordWriter(store)
    flags.read && return RecordReader(store)
    throw(ArgumentError("Unsupported flags: $flags"))
end

function Base.open(store::RecordStore, mode::AbstractString)
    mode == "w" && return open(store, write=true)
    mode == "r" && return open(store, read=true)
    throw(ArgumentError("Unsupported mode: $mode"))
end

Base.read(store::RecordStore) = open(read, store)

struct RecordWriter
    path::String
    writer
    counter::typeof(Ref(0))
end

RecordWriter(path::AbstractString, writer) = RecordWriter(path, writer, Ref(0))
RecordWriter(store::RecordStore) = RecordWriter(store.path, store.writer(store.path))

Base.close(w::RecordWriter) = close(w.writer)
Base.flush(w::RecordWriter) = flush(w.writer)

maybeclose(_) = nothing
maybeclose(io::IO) = close(io)

function Base.write(w::RecordWriter, obj)
    i = w.counter[] += 1
    f = addfile(w.writer, "$i.bson")
    obj = deepcopy(obj)  # https://github.com/MikeInnes/BSON.jl/issues/26
    try
        BSON.bson(f, obj)
    finally
        maybeclose(f)  # flush
    end
    return w
end

struct RecordReader
    path::String
    reader
end

RecordReader(path::AbstractString, reader) = RecordReader(path, reader)
RecordReader(store::RecordStore) = RecordReader(store.path, store.reader(store.path))

Base.close(r::RecordReader) = close(r.reader)

fileid(x) = parse(Int, chop(filename(x); tail = length(".bson")))
sortedfiles(r::RecordReader) = sort!(collect(files(r.reader)); by = fileid)

function Base.read(r::RecordReader)
    return mapfoldl(BSON.load, push!!, sortedfiles(r); init = Union{}[])
end

Base.iterate(r::RecordReader) = iterate(r, (sortedfiles(r), 1))
function Base.iterate(r::RecordReader, (files, i))
    i > length(files) && return nothing
    return (BSON.load(files[i]), (files, i + 1))
end

Base.IteratorSize(::Type{<:RecordReader}) = Base.SizeUnknown()
Base.IteratorEltype(::Type{<:RecordReader}) = Base.EltypeUnknown()

end # module
