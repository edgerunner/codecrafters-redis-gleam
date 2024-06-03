import gleam/io
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
  |> io.debug
  |> should.be_ok
  |> pair.first
  |> should.equal(resp.Array([resp.BulkString("ECHO"), resp.BulkString("hey")]))
}

pub fn fail_parse_array_with_wrong_length_test() {
  <<"*3\r\n$4\r\nECHO\r\n$3\r\nhey\r\n":utf8>>
  |> resp.parse
  |> io.debug
  |> should.be_error
  |> should.equal(resp.InvalidLength)
}
