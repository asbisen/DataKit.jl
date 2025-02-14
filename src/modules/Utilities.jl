module Utilities

using Downloads
using SHA

export download_file

"""
    download_file(remote_url::AbstractString,
                 local_target::AbstractString=auto_filename(remote_url);
                 overwrite::Bool=false,
                 verify_checksum::Union{String,Nothing}=nothing,
                 create_dirs::Bool=true,
                 progress::Bool=true) -> String

Downloads a file from a remote URL with additional functionality for data analysis workflows.

Arguments:
- `remote_url`: The URL to download from
- `local_target`: Local path to save the file (optional, inferred from URL if not provided)
- `overwrite`: Whether to overwrite existing files
- `verify_checksum`: SHA256 checksum to verify file integrity
- `create_dirs`: Create necessary directories in the path
- `progress`: Show download progress

Returns:
- Path to the downloaded file
"""
function download_file(remote_url::AbstractString,
    local_target::AbstractString=auto_filename(remote_url);
    overwrite::Bool=false,
    verify_checksum::Union{String,Nothing}=nothing,
    create_dirs::Bool=true)

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
