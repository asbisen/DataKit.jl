using DataKit.DuckDatabase


@testset "DuckDatabase #1" begin
    dbfile = "dbtest.duckdb"
    db = DDB(dbfile)

    @test isfile(dbfile) == false
    @test isconnected(db) == false
    connect_database!(db)
    @test isconnected(db) == true
    @test isfile(dbfile) == true

    tbls = list_tables(db)
    @test length(tbls) == 0

    close_database!(db)
    @test isconnected(db) == false
    rm(dbfile)
    @test isfile(dbfile) == false
end
