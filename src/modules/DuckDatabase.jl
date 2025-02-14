module DuckDatabase

using DataFrames
using DuckDB
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
    isconnected(db) || return DataFrame()
    con = connect(db.db)
    df = execute(con, query) |> DataFrame
    close!(con)
    return df
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
