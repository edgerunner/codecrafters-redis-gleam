import gleeunit/should
import redis/resp

pub fn parse_simple_string_pong_test() {
  <<"+PONG\r\n":utf8>>
  |> resp.parse
  |> should.be_ok
  |> should.equal(resp.SimpleString("PONG"))
}

pub fn fail_parse_simple_string_with_crlf_test() {
  <<"+PO\r\nNG\r\n":utf8>>
  |> resp.parse
  |> should.be_error
  |> should.equal(resp.UnexpectedInput(<<"\r\nNG\r\n":utf8>>))
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
