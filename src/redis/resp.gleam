pub type Resp {
  SimpleString(String)
  BulkString(String)
  Array(List(Resp))
}

pub type ParseError {
  ParseError
  UnexpectedInput(BitArray)
  UnexpectedEnd
}

import gleam/bit_array
import gleam/result
import gleam/string

const crlf = <<"\r\n":utf8>>

pub fn parse(input: BitArray) -> Result(Resp, ParseError) {
  case input {
    <<"+":utf8, rest:bits>> -> parse_simple_string(rest)
    <<"$":utf8, rest:bits>> -> todo
    <<"*":utf8, rest:bits>> -> todo
    unexpected -> Error(UnexpectedInput(unexpected))
  }
}

fn parse_simple_string(input: BitArray) -> Result(Resp, ParseError) {
  case input {
    <<"\r\n":utf8>> -> Ok(SimpleString(""))
    <<"\r":utf8>> | <<"\n":utf8>> | <<>> -> Error(UnexpectedEnd)
    <<"\r":utf8, _rest:bits>> | <<"\n":utf8, _rest:bits>> ->
      Error(UnexpectedInput(input))
    <<char:utf8_codepoint, rest:bits>> -> {
      use tail_resp: Resp <- result.map(parse_simple_string(rest))
      let assert SimpleString(tail) = tail_resp
      let head = string.from_utf_codepoints([char])
      SimpleString(head <> tail)
    }

    unexpected -> Error(UnexpectedInput(unexpected))
  }
}
