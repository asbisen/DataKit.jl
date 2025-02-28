using DataKit.TextEncoding

@testset "TextEncoding #1" begin
    config = EncodingConfig()

    # Test string with Latin-1 encoding
    latin1_string = "Se\xf1or"
    @test detect_encoding(latin1_string) == Latin1

    win1252_string = "Smart \x93quotes\x94"
    @test detect_encoding(win1252_string) == Windows1252

    # Mixed String
    mixed_string = "This symbol\xa9 with Se\xf1or"
    @test detect_encoding(mixed_string) == Latin1
    fixed_string = fix_encoding(mixed_string)
    @test detect_encoding(fixed_string) == UTF8

end
