module Utilities

using Downloads
using Dates
using Glob
using SHA

export download_file, auto_filename, glob_files

"""
    download_file(remote_url::AbstractString,
                 local_target::AbstractString=auto_filename(remote_url);
                 overwrite::Bool=false,
                 verify_checksum::Union{String,Nothing}=nothing,
                 create_dirs::Bool=true) -> String

Downloads a file from a remote URL with additional functionality for data analysis workflows.
The function supports both direct file paths and directory paths as the download target.

# Arguments
- `remote_url`: The URL to download from
- `local_target`: Local path to save the file. Can be either a specific filename or a directory path
- `overwrite`: Whether to overwrite existing files (default: false)
- `verify_checksum`: SHA256 checksum to verify file integrity (default: nothing)
- `create_dirs`: Create necessary directories in the path (default: true)

# Returns
- Path to the downloaded file as a string

# Examples
Download with a specific filename:
```julia
# Will save as "myfile.csv" in the specified path
download_file("https://example.com/data.csv", "local/path/myfile.csv")
```

Download into a directory (filename derived from URL):
```julia
# Both will save as "data.csv" in the specified directory
download_file("https://example.com/data.csv", "local/path/")
download_file("https://example.com/data.csv", "local/path")
```

Download with checksum verification:
```julia
download_file("https://example.com/data.csv", "data.csv",
             verify_checksum="abc123...") # SHA256 hash
```

# Notes
- If `local_target` is a directory path, the filename will be automatically derived from the URL
- Directories in the path will be created automatically unless `create_dirs=false`
- Existing files won't be overwritten unless `overwrite=true`
- Progress is displayed during download
- Throws an error if the download fails or if checksum verification fails
"""
function download_file(remote_url::AbstractString,
    local_target::AbstractString=auto_filename(remote_url);
    overwrite::Bool=false,
    verify_checksum::Union{String,Nothing}=nothing,
    create_dirs::Bool=true)

    # If local_target is a directory, append auto-generated filename
    if isdir(local_target) || endswith(local_target, '/') || endswith(local_target, '\\')
        # Ensure the path ends with a directory separator
        dir_path = rstrip(local_target, ['/', '\\']) * '/'
        # Generate filename from URL
        filename = auto_filename(remote_url)
        # Combine directory path with filename
        local_target = joinpath(dir_path, filename)
    end

    # Create directories if needed
    if create_dirs
        mkpath(dirname(local_target))
    end

    # Check if file exists
    if isfile(local_target) && !overwrite
        @info "File already exists at $(local_target) and overwrite is false"
        return local_target
    end

    try
        # Download with progress
        Downloads.download(remote_url, local_target)

        # Verify checksum if provided
        if !isnothing(verify_checksum)
            local_checksum = bytes2hex(open(sha256, local_target))
            if local_checksum != verify_checksum
                error("Checksum verification failed!")
            end
        end

        @info "File successfully downloaded to $(local_target)"
        return local_target

    catch e
        @error "Download failed" exception = e
        rethrow(e)
    end
end


"""
    auto_filename(url::AbstractString) -> String

Infers filename from URL, handling various edge cases.
"""
function auto_filename(url::AbstractString)
    parts = split(url, '/')
    filename = last(parts)

    if isempty(filename)
        error("Could not infer filename from URL: $url")
    end

    # Remove query parameters if present
    filename = split(filename, '?')[1]

    return filename
end



"""
    glob_files(directory::AbstractString, pattern::AbstractString;
               files_only::Bool=true, dirs_only::Bool=false,
               return_relative::Bool=false,
               min_size::Int=0, max_size::Int=typemax(Int),
               modified_after::DateTime=DateTime(0),
               modified_before::DateTime=DateTime(3000))

Enhanced search for files in the specified directory matching the given pattern.

# Arguments
- `directory::AbstractString`: The directory to search in
- `pattern::AbstractString`: The glob pattern to match files against

# Keyword Arguments
- `files_only::Bool=true`: Whether to return only files (not directories)
- `dirs_only::Bool=false`: Whether to return only directories (not files)
- `return_relative::Bool=false`: Return paths relative to the search directory
- `min_size::Int=0`: Minimum file size in bytes
- `max_size::Int=typemax(Int)`: Maximum file size in bytes
- `modified_after::DateTime=DateTime(0)`: Only files modified after this time
- `modified_before::DateTime=DateTime(3000)`: Only files modified before this time

# Returns
- An array of strings containing paths of all matching files or directories

# Examples
```julia
# Find all .txt files in the current directory
txt_files = glob_files(".", "*.txt")

# Find all .jpg files in the images directory
image_files = glob_files("images", "*.jpg")

# Find all files matching a pattern in a specific directory
data_files = glob_files("/path/to/data", "*.csv")

# Find only directories
folders = glob_files("project", "*", dirs_only=true)

# Find files modified in the last 24 hours
recent_files = glob_files("logs", "*.log",
                         modified_after=Dates.now() - Dates.Day(1))
```
"""
function glob_files(directory::AbstractString, pattern::AbstractString;
    files_only::Bool=true, dirs_only::Bool=false,
    return_relative::Bool=false,
    min_size::Int=0, max_size::Int=typemax(Int),
    modified_after::DateTime=DateTime(0),
    modified_before::DateTime=DateTime(3000))

    # Input validation
    if dirs_only && files_only
        error("Cannot set both dirs_only and files_only to true")
    end

    # Check if directory exists
    if !isdir(directory)
        error("Directory does not exist: $directory")
    end

    # Normalize directory path
    norm_directory = normpath(directory) |> abspath

    try
        # For directory-only search, append a trailing slash to the pattern
        search_pattern = dirs_only ? pattern * "/" : pattern

        # Call glob with pattern and directory
        matching_paths = glob(search_pattern, norm_directory)

        # Apply filters
        filtered_paths = filter(matching_paths) do path
            # File vs directory filter (if not already handled by the pattern)
            is_dir = isdir(path)
            if files_only && is_dir
                return false
            elseif dirs_only && !is_dir
                return false
            end

            # Size filters (for files only)
            if !is_dir
                file_size = filesize(path)
                if file_size < min_size || file_size > max_size
                    return false
                end
            end

            # Modification time filters
            file_mtime = DateTime(Dates.unix2datetime(mtime(path)))
            if file_mtime < modified_after || file_mtime > modified_before
                return false
            end

            return true
        end

        # Convert to relative paths if requested
        if return_relative
            return [relpath(path, norm_directory) for path in filtered_paths]
        else
            return filtered_paths
        end
    catch e
        @warn "Error during file globbing: $e"
        return String[]
    end
end



function glob_files(path_pattern::AbstractString;
    files_only::Bool=true,
    dirs_only::Bool=false,
    return_relative::Bool=false,
    min_size::Int=0,
    max_size::Int=typemax(Int),
    modified_after::DateTime=DateTime(0),
    modified_before::DateTime=DateTime(3000))

    # Split the path pattern into directory and file pattern
    wildcards = ['*', '?', '[', '{']

    # Check if there are any wildcards in the path
    wildcard_found = false
    wildcard_pos = length(path_pattern) + 1  # Default to after end of string

    for w in wildcards
        pos = findfirst(string(w), path_pattern)
        if pos !== nothing
            if pos[1] < wildcard_pos
                wildcard_pos = pos[1]
                wildcard_found = true
            end
        end
    end

    if !wildcard_found
        # No wildcards, treat as literal path
        if isdir(path_pattern)
            directory = path_pattern
            pattern = "*"
        else
            directory = dirname(path_pattern)
            pattern = basename(path_pattern)
        end
    else
        # Find last directory separator before the first wildcard
        last_sep_pos = 0
        for i in 1:wildcard_pos-1
            if path_pattern[i] == '/' || path_pattern[i] == '\\'
                last_sep_pos = i
            end
        end

        if last_sep_pos == 0
            # No directory separators before wildcard
            directory = "."
            pattern = path_pattern
        else
            directory = path_pattern[1:last_sep_pos]
            pattern = path_pattern[last_sep_pos+1:end]
        end
    end

    # Call the original glob_files function with separated components
    return glob_files(directory, pattern;
        files_only=files_only,
        dirs_only=dirs_only,
        return_relative=return_relative,
        min_size=min_size,
        max_size=max_size,
        modified_after=modified_after,
        modified_before=modified_before)
end



end # module
