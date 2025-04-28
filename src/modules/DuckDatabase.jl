module DuckDatabase

using DataFrames
using DuckDB
using Dates
import DBInterface: connect, execute, close!


export DDB,
    connect_database!,
    close_database!,
    querydf,
    list_tables,
    hastable,
    droptable!,
    list_databases,
    isconnected


mutable struct DDB
    dbfile::String
    db::Union{DuckDB.DB,Nothing}
end

DDB(dbfile::String)::DDB = DDB(dbfile, nothing)
isconnected(db::DDB)::Bool = !isnothing(db.db)



"""
    connect_database!(db::DDB)::Bool

Establish connection to DuckDB database file. Returns true when successful.
"""
function connect_database!(db::DDB)::Bool
    db.db = DuckDB.DB(db.dbfile)
    return true
end



"""
    close_database!(db::DDB)::Bool

Close connection to DuckDB database if connected. Returns true when successful
or if already disconnected.
"""
function close_database!(db::DDB)::Bool
    isconnected(db) || return true
    DuckDB.close_database(db.db)
    db.db = nothing
    return true
end



"""
    querydf(db::DDB, query::String)::DataFrame

Execute SQL query on database and return results as DataFrame.
Returns empty DataFrame if database not connected.

# Arguments
- `db`: DuckDB database connection
- `query`: SQL query string to execute

# Returns
DataFrame containing query results
"""
function querydf(db::DDB, query::String)::DataFrame
    isconnected(db) || connect_database!(db)
    con = connect(db.db)
    df = execute(con, query) |> DataFrame
    close!(con)
    return df
end



"""
    querydf(dbfile::String, query::String; verbose::Bool=false)::DataFrame

Execute a SQL query against a DuckDB database and return the results as a DataFrame.

# Arguments
- `dbfile::String`: Path to the database file or ":memory:" for an in-memory database.
- `query::String`: SQL query to execute.
- `verbose::Bool=false`: If true, prints detailed information about the query execution process.

# Returns
- `DataFrame`: Results of the query as a DataFrame.

# Throws
- `ArgumentError`: If the query is empty or if the database file doesn't exist.

# Examples
```julia
# Query from a file-based database
df = querydf("mydata.duckdb", "SELECT * FROM mytable")

# Query from in-memory database with verbose output
df = querydf(":memory:", "SELECT 1 AS value", verbose=true)
```
"""

function querydf(dbfile::String, query::String;
    verbose::Bool=false,
    profile::Bool=false)::DataFrame

    # Validate query is not empty
    if isempty(query)
        throw(ArgumentError("Query cannot be empty"))
    end

    # Validate dbfile exists unless it's ":memory:"
    if dbfile != ":memory:" && !isfile(dbfile)
        throw(ArgumentError("Database file '$dbfile' does not exist"))
    end

    if verbose
        println("Opening database: $dbfile")
        println("Executing query: $query")
    end

    db = DDB(dbfile, nothing)
    connect_database!(db)

    # Performance tracking variables
    local start_time, end_time, df_size_mb, memory_before, memory_after
    local plan_df, execution_stats

    try
        # Profile query plan if requested
        if profile || verbose
            plan_df = querydf(db, "EXPLAIN ANALYZE $query")
            memory_before = Sys.free_memory() / 1024 / 1024  # MB
        end

        if verbose
            println("Executing query...")
            start_time = time()
        end

        # Execution timing
        start_time = time_ns()
        df = querydf(db, query)
        end_time = time_ns()

        # Calculate performance metrics
        if profile || verbose
            execution_time_ms = (end_time - start_time) / 1_000_000  # ns to ms
            df_size_mb = Base.summarysize(df) / 1024 / 1024  # Convert bytes to MB
            memory_after = Sys.free_memory() / 1024 / 1024  # MB
            memory_impact = memory_before - memory_after

            # Calculate rows processed per second
            rows_per_second = nrow(df) / (execution_time_ms / 1000)

            # Get additional execution statistics if profiling
            # if profile
            #     execution_stats = DuckDB.execute(con, "SELECT * FROM duckdb_profiles() ORDER BY duration DESC LIMIT 5") |> DataFrame
            # end
        end

        # Display performance information with improved layout
        if verbose || profile
            println("\n" * "="^60)
            println("📊 QUERY PERFORMANCE SUMMARY")
            println("="^60)

            println("🕒 Timing:")
            println("   ├─ Execution time: $(round(execution_time_ms, digits=2)) ms")
            println("   └─ Throughput: $(round(Int, rows_per_second)) rows/sec")

            println("\n📋 Results:")
            println("   ├─ Rows: $(nrow(df))")
            println("   ├─ Columns: $(ncol(df))")
            println("   └─ Size: $(round(df_size_mb, digits=2)) MB")

            println("\n🧠 Memory:")
            println("   ├─ Impact: $(round(memory_impact, digits=2)) MB")
            println("   └─ Result set: $(round(df_size_mb, digits=2)) MB")

            if profile
                println("\n📝 Query Plan:")
                # Format the query plan for better readability
                if !isempty(plan_df)
                    plan_text = plan_df[1, 2]
                    # Add indentation to plan text for better hierarchy visualization
                    println(join(map(line -> "   " * line, split(plan_text, "\n")), "\n"))
                end

                # println("\n⚡ Top Operations (by duration):")
                # if !isempty(execution_stats)
                #     for i in 1:min(nrow(execution_stats), 5)
                #         op = execution_stats[i, :name]
                #         dur = execution_stats[i, :duration]
                #         println("   $(i). $(op): $(round(dur, digits=2)) ms")
                #     end
                # end
            end

            println("="^60)
        end

        return df

    finally
        close_database!(db)
    end
end




"""
    list_tables(db::DDB)::Vector{String}

Returns names of all tables in the connected database.
Returns empty vector if not connected.
"""
function list_tables(db::DDB)::Vector{String}
    isconnected(db) || return Vector{String}()
    con = connect(db.db)
    res = execute(con, "SHOW TABLES;") |> DataFrame
    close!(con)
    return res[!, :name]
end



"""
    hastable(db::DDB, table_name::String)::Bool

Check if a table exists in the database, ignoring case.
"""
function hastable(db::DDB, table_name::String)::Bool
    tables = lowercase.(list_tables(db))
    return lowercase(table_name) in tables
end



"""
    droptable!(db::DDB, table_name::String)::Bool

Drops the specified table from the database if it exists. Returns true if successful
or if table doesn't exist.
"""
function droptable!(db::DDB, table_name::String)::Bool
    if hastable(db, table_name)
        con = connect(db.db)
        execute(con, "DROP TABLE IF EXISTS $table_name;")
        close!(con)
        return true
    else
        return true
    end
end


"""
    list_databases(db::DDB)::Vector{String}

Returns a vector of database names in the connected DuckDB instance.
If not connected, returns an empty vector.
"""
function list_databases(db::DDB)::Vector{String}
    isconnected(db) || return Vector{String}()
    con = connect(db.db)
    res = execute(con, "SHOW DATABASES;") |> DataFrame
    close!(con)
    return res[!, :database_name]
end


end # module
