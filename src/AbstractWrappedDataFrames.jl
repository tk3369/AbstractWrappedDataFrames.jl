module AbstractWrappedDataFrames

export AbstractWrappedDataFrame

using DataFrames
using Lazy: @forward

abstract type AbstractWrappedDataFrame <: AbstractDataFrame end

# Functions for AbstractWrappedDataFrame

# TODO Not ideal to hard code `df` field
dataframe(sf::AbstractWrappedDataFrame) = getfield(sf, :df)

# Implement the "unofficial" AbstractDataFrame interface
# See https://github.com/invenia/KeyedFrames.jl/issues/19#issuecomment-674753267

# Using Lazy.jl, we can forward these functions to the underlying data frame.

@forward AbstractWrappedDataFrame.df Base.getindex
@forward AbstractWrappedDataFrame.df Base.setindex!
@forward AbstractWrappedDataFrame.df Base.propertynames
@forward AbstractWrappedDataFrame.df Base.push!
@forward AbstractWrappedDataFrame.df Base.copy
@forward AbstractWrappedDataFrame.df Base.empty!
@forward AbstractWrappedDataFrame.df Base.hcat
@forward AbstractWrappedDataFrame.df Base.vcat
@forward AbstractWrappedDataFrame.df Base.sort!
@forward AbstractWrappedDataFrame.df Base.append!
@forward AbstractWrappedDataFrame.df Base.delete!
@forward AbstractWrappedDataFrame.df Base.parent
@forward AbstractWrappedDataFrame.df Base.parentindices

@forward AbstractWrappedDataFrame.df DataFrames.insertcols!
@forward AbstractWrappedDataFrame.df DataFrames.ncol
@forward AbstractWrappedDataFrame.df DataFrames.nrow
@forward AbstractWrappedDataFrame.df DataFrames.transform!
@forward AbstractWrappedDataFrame.df DataFrames.select!
@forward AbstractWrappedDataFrame.df DataFrames.index

@forward AbstractWrappedDataFrame.df DataFrames.DataFrameRow
# @forward AbstractWrappedDataFrame.df DataFrames.DataFrameRows
@forward AbstractWrappedDataFrame.df DataFrames.SubDataFrame
# @forward AbstractWrappedDataFrame.df DataFrames.GroupedDataFrame
# @forward AbstractWrappedDataFrame.df DataFrames.DataFrameColumns

# TODO This is not ideal because I would rather return only the properties
# of the underlying data frame. But if I don't support getting the object's
# own properties then I run into other problems with the `parent` function.
function Base.getproperty(sf::T, s::Symbol) where {T <: AbstractWrappedDataFrame}
    if s ∈ fieldnames(T)
        return getfield(sf, s)
    else
        return getproperty(dataframe(sf), s)
    end
end

# Custom forwarders to avoid ambiguity

# Avoid ambiguity since
# 1) Base defines setproperty!(::Any, ::Symbol, ::Any)
# 2) @forward defines setproperty!(::AbstractWrappedDataFrame, args...; kwargs...)
function Base.setproperty!(sf::AbstractWrappedDataFrame, s::Symbol, v::Any)
    return setproperty!(dataframe(sf), s, v)
end

# required by REPL completion.
# or get this error "propertynames(::SF, ::Bool) is ambiguous."
function Base.propertynames(sf::AbstractWrappedDataFrame, private::Bool)
    return propertynames(dataframe(sf), private)
end

# required by all joins
function Base.convert(::Type{S}, df::D) where {S <: AbstractWrappedDataFrame, D <: AbstractDataFrame}
    return S(df)
end

# required by eachcol
function Base.convert(::Type{D}, sf::S) where {S <: AbstractWrappedDataFrame, D <: AbstractDataFrame}
    return dataframe(sf)
end

# required by eachcol
function Base.convert(::Type{S}, sf::S) where {S <: AbstractWrappedDataFrame}
    return sf
end

# required by row indexing to return DataFrameRow
#=
julia> wdf[1, :]
ERROR: MethodError: getindex(::WDF, ::Int64, ::Colon) is ambiguous. Candidates:
  getindex(df::AbstractDataFrame, rowind::Integer, ::Colon) in DataFrames at /Users/tomkwong/.julia/packages/DataFrames/cdZCk/src/dataframerow/dataframerow.jl:128
  getindex(x::AbstractWrappedDataFrame, args...; kwargs...) in AbstractWrappedDataFrames at /Users/tomkwong/.julia/packages/Lazy/9Xnd3/src/macros.jl:297
  getindex(df::AbstractDataFrame, rowind::Integer, colinds::Union{Colon, Regex, AbstractArray{T,1} where T, All, Between, InvertedIndex}) in DataFrames at /Users/tomkwong/.julia/packages/DataFrames/cdZCk/src/dataframerow/dataframerow.jl:126
Possible fix, define
  getindex(::AbstractWrappedDataFrame, ::Integer, ::Colon)
=#
for v in [Colon, DataFrames.MultiColumnIndex]
    @eval function Base.getindex(s::AbstractWrappedDataFrame, i::Integer, arg::$v)
        return dataframe(s)[i, arg]
    end
end

# Custom extensions as these functions do not take AbstractDataFrame

using DataFrames: RowGroupDict, MultiColumnIndex

# required by semi/anti-joins
function DataFrames.findrow(gd::RowGroupDict,
                 sf::AbstractWrappedDataFrame,
                 args...)
    return DataFrames.findrow(gd, dataframe(sf), args...)
end

# required by inner/outer/left/right-joins
function DataFrames.findrows(gd::RowGroupDict,
                  sf::AbstractWrappedDataFrame,
                  args...)
    return DataFrames.findrows(gd, dataframe(sf), args...)
end

# required by select and many others
function DataFrames.manipulate(sf::AbstractWrappedDataFrame, args...; kwargs...)
    return DataFrames.manipulate(dataframe(sf), args...; kwargs...)
end

# required for groupby
# function DataFrames.SubDataFrame(sf::AbstractWrappedDataFrame, args...; kwargs...)
#     return DataFrames.SubDataFrame(dataframe(sf), args...; kwargs...)
# end

# required for `wdf[1, Between(:x, :y)] .= 4`
# function DataFrames.DataFrameRow(sf::AbstractWrappedDataFrame, args...; kwargs...)
#     return DataFrames.DataFrameRow(dataframe(sf), args...; kwargs...)
# end


end
