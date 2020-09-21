module TestBasicOperations

using AbstractWrappedDataFrames
using DataFrames
using Test

struct WDF <: AbstractWrappedDataFrame
    df::DataFrame
end

struct NDF <: AbstractWrappedDataFrame
    df::DataFrame
    name::String
end

# This is required to support various DataFrames join operations
NDF(df::DataFrame) = NDF(df, "No name")

function test_operations(label::AbstractString, make_wdf::Function, args...)

    @testset "$label" begin

        df1 = make_wdf(DataFrame(x = [1,2], y = [4,5]), args...)
        df2 = make_wdf(DataFrame(x = [1,4], z = [7,8]), args...)

        @testset "Metadata" begin
            @test sort(names(df1)) == ["x", "y"]
            @test sort(string.(propertynames(df1))) == ["x", "y"]
            @test nrow(df1) == 2
            @test ncol(df1) == 2
        end

        @testset "Filter" begin
            @test first(df1, 1).x == [1]
            @test last(df1, 1).x == [2]
            @test filter(:x => ==(1), df1) |> nrow == 1
        end

        @testset "Add columns" begin
            sf3 = copy(df1)
            @test_nowarn sf3.z1 = 10
            @test_nowarn sf3.z2 = [10,11]
            @test_nowarn sf3.z3 = sf3.z2 .* 2
        end

        @testset "Summarize" begin
            @test select(df1, :x) |> ncol == 1
            @test transform(df1, :x => (x -> x .+ 1) => :q) |> ncol == 3
            @test combine(df1, :x => sum) |> nrow == 1
            @test groupby(df1, :x) |> keys |> length == 2
        end

        @testset "Joins" begin
            @test innerjoin(df1, df2, on = :x) isa DataFrame
            @test leftjoin(df1, df2, on = :x) isa DataFrame
            @test rightjoin(df1, df2, on = :x) isa DataFrame
            @test outerjoin(df1, df2, on = :x) isa DataFrame
            @test semijoin(df1, df2, on = :x) isa DataFrame
            @test antijoin(df1, df2, on = :x) isa DataFrame
        end

        @testset "Return types" begin
            @test select(df1, :x) isa DataFrame
            @test transform(df1, :x => (x -> x .+ 1) => :q) isa DataFrame
            @test combine(df1, :x => sum) isa DataFrame
            @test groupby(df1, :x) isa GroupedDataFrame

            sf3 = copy(df1)
            @test select!(sf3, :x) isa DataFrame
            @test transform!(sf3, :x => identity) isa DataFrame
        end
    end
end

function test()
    @testset "Basic Operations" begin
        test_operations("Simple", df -> WDF(df))
        test_operations("Complex", df -> NDF(df, "MyName"))
    end
end

end # module

using .TestBasicOperations
TestBasicOperations.test()
