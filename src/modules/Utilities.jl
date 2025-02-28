module Utilities

using Downloads
using SHA

export download_file, auto_filename

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


end # module
