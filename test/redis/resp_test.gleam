import gleam/pair
import gleeunit/should
import redis/resp

pub fn parse_simple_string_pong_test() {
  <<"+PONG\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.SimpleString("PONG"))
}

pub fn parse_simple_string_with_early_crlf_test() {
  <<"+PO\r\nNG\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.SimpleString("PO"))
}

pub fn fail_parse_simple_string_abrupt_end_test() {
  <<"+PONG":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.UnexpectedEnd)
}

pub fn fail_parse_simple_string_invalid_end_test() {
  <<"+PONG\n":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.UnexpectedEnd)
}

pub fn parse_simple_error_test() {
  <<"-WRONG\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.SimpleError("WRONG"))
}

pub fn parse_bulk_string_echo_test() {
  <<"$4\r\nECHO\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.BulkString("ECHO"))
}

pub fn fail_parse_bulk_string_without_terminator_test() {
  <<"$4\r\nECHO":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.UnexpectedEnd)
}

pub fn fail_parse_bulk_string_with_longer_length_test() {
  <<"$8\r\nECHO\r\n":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.InvalidLength)
}

pub fn parse_array_echo_hey_test() {
  <<"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.Array([resp.BulkString("ECHO"), resp.BulkString("hey")]))
}

pub fn fail_parse_array_with_wrong_length_test() {
  <<"*3\r\n$4\r\nECHO\r\n$3\r\nhey\r\n":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.InvalidLength)
}

pub fn fail_parse_array_with_failing_element_test() {
  <<"*3\r\n+ECHO\r\n+he\ny\r\n":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.UnexpectedInput(<<"\ny\r\n":utf8>>))
}

pub fn parse_null_test() {
  <<"_\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.Null(resp.NullPrimitive))
}

pub fn fail_parse_null_with_content_test() {
  <<"_no!\r\n":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.UnexpectedInput(<<"no!\r\n":utf8>>))
}

pub fn parse_null_string_test() {
  <<"$-1\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.Null(resp.NullString))
}

pub fn parse_null_array_test() {
  <<"*-1\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.Null(resp.NullArray))
}

pub fn encode_simple_string_pong_test() {
  resp.SimpleString("PONG")
  |> resp.encode
  |> should.equal(<<"+PONG\r\n":utf8>>)
}

pub fn encode_simple_error_wrong_test() {
  resp.SimpleError("WRONG")
  |> resp.encode
  |> should.equal(<<"-WRONG\r\n":utf8>>)
}

pub fn encode_null_test() {
  resp.Null(resp.NullPrimitive)
  |> resp.encode
  |> should.equal(<<"_\r\n":utf8>>)
}

pub fn encode_bulk_string_echo_test() {
  resp.BulkString("ECHO")
  |> resp.encode
  |> should.equal(<<"$4\r\nECHO\r\n":utf8>>)
}

pub fn encode_empty_array_test() {
  resp.Array([])
  |> resp.encode
  |> should.equal(<<"*0\r\n":utf8>>)
}

pub fn encode_array_of_simple_strings_test() {
  resp.Array([resp.SimpleString("Hello"), resp.SimpleString("World!")])
  |> resp.encode
  |> should.equal(<<"*2\r\n+Hello\r\n+World!\r\n":utf8>>)
}

pub fn encode_nested_array_test() {
  resp.Array([
    resp.SimpleString("ECHO"),
    resp.Array([resp.BulkString("Hello"), resp.BulkString("World!")]),
    resp.Null(resp.NullPrimitive),
  ])
  |> resp.encode
  |> should.equal(<<
    "*3\r\n+ECHO\r\n*2\r\n$5\r\nHello\r\n$6\r\nWorld!\r\n_\r\n":utf8,
  >>)
}

pub fn convert_simple_string_to_string_test() {
  resp.SimpleString("Great")
  |> resp.to_string
  |> should.be_ok
  |> should.equal("Great")
}

pub fn convert_bulk_string_to_string_test() {
  resp.SimpleString("Wonderful\r\nworld")
  |> resp.to_string
  |> should.be_ok
  |> should.equal("Wonderful\r\nworld")
}

pub fn dont_convert_array_to_string_test() {
  resp.Array([])
  |> resp.to_string
  |> should.be_error
}
