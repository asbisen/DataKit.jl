module TextEncoding

using Logging


export detect, fix, Latin1, Windows1252, EncodingConfig, UTF8,
    detect_encoding, fix_encoding

# Core types and interfaces
#------------------------

abstract type Encoding end

struct EncodingConfig
    verbose::Bool
    strict::Bool
    fallback_char::Char

    EncodingConfig(;
        verbose=false,
        strict=false,
        fallback_char='�'
    ) = new(verbose, strict, fallback_char)
end

struct EncodingError <: Exception
    message::String
end

# Specific Encoding Types
#-----------------------

struct Latin1 <: Encoding end
struct Windows1252 <: Encoding end
struct UTF8 <: Encoding end

# Character Maps and Constants
#---------------------------

# Latin-1 character map (0x80-0xFF)
# This is ISO-8859-1 standard, all characters from 0xA0-0xFF are valid Unicode points
const LATIN1_CHAR_MAP = Dict{UInt8,Char}(
    # Control chars (0x80-0x9F) are undefined in Latin-1
    # 0xA0-0xFF directly map to Unicode code points U+00A0 to U+00FF
    0xA0 => '\u00A0', # NO-BREAK SPACE
    0xA1 => '¡', 0xA2 => '¢', 0xA3 => '£', 0xA4 => '¤', 0xA5 => '¥',
    0xA6 => '¦', 0xA7 => '§', 0xA8 => '¨', 0xA9 => '©', 0xAA => 'ª',
    0xAB => '«', 0xAC => '¬', 0xAD => '\u00AD', 0xAE => '®', 0xAF => '¯',
    0xB0 => '°', 0xB1 => '±', 0xB2 => '²', 0xB3 => '³', 0xB4 => '´',
    0xB5 => 'µ', 0xB6 => '¶', 0xB7 => '·', 0xB8 => '¸', 0xB9 => '¹',
    0xBA => 'º', 0xBB => '»', 0xBC => '¼', 0xBD => '½', 0xBE => '¾',
    0xBF => '¿', 0xC0 => 'À', 0xC1 => 'Á', 0xC2 => 'Â', 0xC3 => 'Ã',
    0xC4 => 'Ä', 0xC5 => 'Å', 0xC6 => 'Æ', 0xC7 => 'Ç', 0xC8 => 'È',
    0xC9 => 'É', 0xCA => 'Ê', 0xCB => 'Ë', 0xCC => 'Ì', 0xCD => 'Í',
    0xCE => 'Î', 0xCF => 'Ï', 0xD0 => 'Ð', 0xD1 => 'Ñ', 0xD2 => 'Ò',
    0xD3 => 'Ó', 0xD4 => 'Ô', 0xD5 => 'Õ', 0xD6 => 'Ö', 0xD7 => '×',
    0xD8 => 'Ø', 0xD9 => 'Ù', 0xDA => 'Ú', 0xDB => 'Û', 0xDC => 'Ü',
    0xDD => 'Ý', 0xDE => 'Þ', 0xDF => 'ß', 0xE0 => 'à', 0xE1 => 'á',
    0xE2 => 'â', 0xE3 => 'ã', 0xE4 => 'ä', 0xE5 => 'å', 0xE6 => 'æ',
    0xE7 => 'ç', 0xE8 => 'è', 0xE9 => 'é', 0xEA => 'ê', 0xEB => 'ë',
    0xEC => 'ì', 0xED => 'í', 0xEE => 'î', 0xEF => 'ï', 0xF0 => 'ð',
    0xF1 => 'ñ', 0xF2 => 'ò', 0xF3 => 'ó', 0xF4 => 'ô', 0xF5 => 'õ',
    0xF6 => 'ö', 0xF7 => '÷', 0xF8 => 'ø', 0xF9 => 'ù', 0xFA => 'ú',
    0xFB => 'û', 0xFC => 'ü', 0xFD => 'ý', 0xFE => 'þ', 0xFF => 'ÿ'
)

# Windows-1252 character map (focused on 0x80-0x9F which differs from Latin-1)
const WINDOWS1252_CHAR_MAP = Dict{UInt8,Char}(
    # Windows-1252 defines characters in 0x80-0x9F range which are control chars in Latin-1
    0x80 => '€', 0x82 => '‚', 0x83 => 'ƒ', 0x84 => '„', 0x85 => '…',
    0x86 => '†', 0x87 => '‡', 0x88 => 'ˆ', 0x89 => '‰', 0x8A => 'Š',
    0x8B => '‹', 0x8C => 'Œ', 0x8E => 'Ž', 0x91 => ''', 0x92 => ''',
    0x93 => '"', 0x94 => '"', 0x95 => '•', 0x96 => '–', 0x97 => '—',
    0x98 => '˜', 0x99 => '™', 0x9A => 'š', 0x9B => '›', 0x9C => 'œ',
    0x9E => 'ž', 0x9F => 'Ÿ'
    # For 0xA0-0xFF, Windows-1252 is identical to Latin-1/ISO-8859-1
)

# Complete Windows-1252 map by merging with Latin-1 map (for 0xA0-0xFF range)
for (byte, char) in LATIN1_CHAR_MAP
    if !haskey(WINDOWS1252_CHAR_MAP, byte)
        WINDOWS1252_CHAR_MAP[byte] = char
    end
end

# Utility Functions
#----------------

"""
    log_replacements(replacements::Dict{UInt8, Int}, char_map::Dict{UInt8, Char}, encoding_name::String)

Helper function to log character replacements when verbose mode is enabled.
"""
function log_replacements(replacements::Dict{UInt8,Int}, char_map::Dict{UInt8,Char}, encoding_name::String)
    if !isempty(replacements)
        details = ["Converting $encoding_name characters to Unicode:"]
        for (byte, count) in replacements
            push!(details, "  0x$(string(byte, base=16, pad=2)) → $(char_map[byte]): $count replacements")
        end
        @info join(details, "\n")
    else
        @info "No $encoding_name characters needed conversion"
    end
end

"""
    try_decode(bytes::Vector{UInt8}, char_map::Dict{UInt8, Char}, fallback_char::Char)

Attempt to decode bytes using the provided character map, replacing unmapped characters.
"""
function try_decode(bytes::Vector{UInt8}, char_map::Dict{UInt8,Char}, fallback_char::Char)
    result = IOBuffer()
    replacements = Dict{UInt8,Int}()

    for byte in bytes
        if byte < 0x80
            # ASCII range - direct mapping
            write(result, Char(byte))
        elseif haskey(char_map, byte)
            # Character in the map
            write(result, char_map[byte])
            replacements[byte] = get(replacements, byte, 0) + 1
        else
            # Unmapped character
            write(result, fallback_char)
        end
    end

    return String(take!(result)), replacements
end


# Generic Interface
#----------------

"""
    detect(::Type{T}, raw_bytes::Vector{UInt8}, config::EncodingConfig) where T <: Encoding

Generic detection interface for encodings using raw bytes.
"""
function detect(::Type{T}, raw_bytes::Vector{UInt8}, config::EncodingConfig) where {T<:Encoding}
    throw(EncodingError("Must implement detect for specific encoding"))
end

"""
    detect(::Type{T}, s::String, config::EncodingConfig) where T <: Encoding

Generic detection interface for encodings.
"""
function detect(::Type{T}, s::String, config::EncodingConfig) where {T<:Encoding}
    return detect(T, Vector{UInt8}(s), config)
end

"""
    fix(::Type{T}, raw_bytes::Vector{UInt8}, config::EncodingConfig) where T <: Encoding

Generic fixing interface for encodings using raw bytes.
"""
function fix(::Type{T}, raw_bytes::Vector{UInt8}, config::EncodingConfig) where {T<:Encoding}
    throw(EncodingError("Must implement fix for specific encoding"))
end

"""
    fix(::Type{T}, s::String, config::EncodingConfig) where T <: Encoding

Generic fixing interface for encodings.
"""
function fix(::Type{T}, s::String, config::EncodingConfig) where {T<:Encoding}
    return fix(T, Vector{UInt8}(s), config)
end

# Latin1 Implementation
#--------------------

function detect(::Type{Latin1}, raw_bytes::Vector{UInt8}, config::EncodingConfig)
    try
        found_chars = Dict{UInt8,Int}()

        for byte in raw_bytes
            # Check for Latin-1 specific range (0xA0-0xFF)
            if 0xA0 <= byte <= 0xFF
                found_chars[byte] = get(found_chars, byte, 0) + 1
            end
            # Check for control chars in 0x80-0x9F which aren't defined in Latin-1
            # but are defined in Windows-1252, to help exclude Windows-1252
            if 0x80 <= byte <= 0x9F && haskey(WINDOWS1252_CHAR_MAP, byte)
                # This suggests Windows-1252 rather than Latin-1
                return false
            end
        end

        if config.verbose && !isempty(found_chars)
            details = ["Found potential Latin-1 encoded bytes:"]
            for (byte, count) in found_chars
                char_desc = haskey(LATIN1_CHAR_MAP, byte) ? " ($(LATIN1_CHAR_MAP[byte]))" : ""
                push!(details, "  0x$(string(byte, base=16, pad=2))$char_desc: $count occurrences")
            end
            @info join(details, "\n")
        end

        return !isempty(found_chars)
    catch e
        throw(EncodingError("Error detecting Latin-1 encoding: $e"))
    end
end

function fix(::Type{Latin1}, raw_bytes::Vector{UInt8}, config::EncodingConfig)
    try
        fixed_text, replacements = try_decode(raw_bytes, LATIN1_CHAR_MAP, config.fallback_char)

        if config.verbose
            log_replacements(replacements, LATIN1_CHAR_MAP, "Latin-1")
        end

        return fixed_text
    catch e
        throw(EncodingError("Error fixing Latin-1 encoding: $e"))
    end
end

# Windows-1252 Implementation
#--------------------------

function detect(::Type{Windows1252}, raw_bytes::Vector{UInt8}, config::EncodingConfig)
    try
        found_latin1_chars = Dict{UInt8,Int}()
        found_win1252_chars = Dict{UInt8,Int}()

        for byte in raw_bytes
            # Check for Windows-1252 specific chars (0x80-0x9F range)
            if 0x80 <= byte <= 0x9F && haskey(WINDOWS1252_CHAR_MAP, byte)
                found_win1252_chars[byte] = get(found_win1252_chars, byte, 0) + 1
            end
            # Also track Latin-1 range for combined reporting
            if 0xA0 <= byte <= 0xFF
                found_latin1_chars[byte] = get(found_latin1_chars, byte, 0) + 1
            end
        end

        if config.verbose
            if !isempty(found_win1252_chars)
                details = ["Found Windows-1252 specific bytes:"]
                for (byte, count) in found_win1252_chars
                    char_desc = haskey(WINDOWS1252_CHAR_MAP, byte) ? " ($(WINDOWS1252_CHAR_MAP[byte]))" : ""
                    push!(details, "  0x$(string(byte, base=16, pad=2))$char_desc: $count occurrences")
                end
                @info join(details, "\n")
            end

            if !isempty(found_latin1_chars)
                @info "Also found $(length(found_latin1_chars)) bytes in shared Latin-1 range (0xA0-0xFF)"
            end
        end

        # If we found Windows-1252 specific chars, it's definitely Windows-1252
        return !isempty(found_win1252_chars) || !isempty(found_latin1_chars)
    catch e
        throw(EncodingError("Error detecting Windows-1252 encoding: $e"))
    end
end

function fix(::Type{Windows1252}, raw_bytes::Vector{UInt8}, config::EncodingConfig)
    try
        fixed_text, replacements = try_decode(raw_bytes, WINDOWS1252_CHAR_MAP, config.fallback_char)

        if config.verbose
            log_replacements(replacements, WINDOWS1252_CHAR_MAP, "Windows-1252")
        end

        return fixed_text
    catch e
        throw(EncodingError("Error fixing Windows-1252 encoding: $e"))
    end
end

# UTF-8 Implementation
#-------------------

function detect(::Type{UTF8}, raw_bytes::Vector{UInt8}, config::EncodingConfig)
    try
        return isvalid(String, raw_bytes)
    catch e
        throw(EncodingError("Error detecting UTF-8 encoding: $e"))
    end
end

function fix(::Type{UTF8}, raw_bytes::Vector{UInt8}, config::EncodingConfig)
    try
        if isvalid(String, raw_bytes)
            return String(raw_bytes)
        end

        # Fix invalid UTF-8 sequences
        result = IOBuffer()
        replacements = 0
        i = 1

        while i <= length(raw_bytes)
            # Check if this byte starts a valid UTF-8 sequence
            valid_len = 0
            for len in 1:4
                if i + len - 1 <= length(raw_bytes) && isvalid(String, raw_bytes[i:i+len-1])
                    valid_len = len
                    break
                end
            end

            if valid_len > 0
                # Valid sequence found
                write(result, raw_bytes[i:i+valid_len-1])
                i += valid_len
            else
                # Invalid sequence - replace with fallback char
                write(result, config.fallback_char)
                replacements += 1
                i += 1
            end
        end

        if config.verbose && replacements > 0
            @info "Fixed $replacements invalid UTF-8 sequences"
        end

        return String(take!(result))
    catch e
        throw(EncodingError("Error fixing UTF-8 encoding: $e"))
    end
end

# Convenience Functions
#--------------------

"""
    detect_encoding(s::Union{String,Vector{UInt8}}, config::EncodingConfig = EncodingConfig())

Detect the likely encoding of a string or byte array.
"""
function detect_encoding(s::Union{String,Vector{UInt8}}, config::EncodingConfig=EncodingConfig())
    bytes = s isa String ? Vector{UInt8}(s) : s

    # First check if it's valid UTF-8
    if detect(UTF8, bytes, config)
        return UTF8
    end

    # Check for Windows-1252 specific characters
    # (Windows-1252 is a superset of Latin-1)
    if detect(Windows1252, bytes, config)
        # We found Windows-1252 specific chars, so prefer that
        # Check if we have any Windows-1252 specific characters (0x80-0x9F range)
        has_win1252_specific = any(b -> 0x80 <= b <= 0x9F && haskey(WINDOWS1252_CHAR_MAP, b), bytes)

        if has_win1252_specific
            return Windows1252
        else
            # Only Latin-1 range characters found
            return Latin1
        end
    end

    # Could not determine encoding
    return nothing
end

"""
    fix_encoding(s::Union{String,Vector{UInt8}}, config::EncodingConfig = EncodingConfig())

Attempt to fix any encoding issues in the string or byte array by trying different encodings.
"""
function fix_encoding(s::Union{String,Vector{UInt8}}, config::EncodingConfig=EncodingConfig())
    bytes = s isa String ? Vector{UInt8}(s) : s
    encoding = detect_encoding(bytes, config)

    if encoding === nothing
        if config.verbose
            @warn "Could not determine encoding, returning original content"
        end
        return s isa String ? s : String(bytes)
    else
        if config.verbose
            @info "Detected encoding: $(encoding)"
        end
        return fix(encoding, bytes, config)
    end
end

end # module
