pub type Resp {
  SimpleString(String)
  BulkString(String)
  Array(List(Resp))
}

pub type ParseError {
  ParseError
  UnexpectedInput(BitArray)
  UnexpectedEnd
  InvalidLength
  InvalidUTF8
}

import gleam/bit_array
import gleam/int
import gleam/result
import gleam/string

const crlf = <<"\r\n":utf8>>

pub fn parse(input: BitArray) -> Result(Resp, ParseError) {
  case input {
    <<"+":utf8, rest:bits>> -> parse_simple_string(rest)
    <<"$":utf8, rest:bits>> -> parse_bulk_string(rest)
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

fn parse_bulk_string(input: BitArray) -> Result(Resp, ParseError) {
  use #(length, rest): #(Int, BitArray) <- result.then(parse_length(input, 0))
  let slice =
    bit_array.slice(from: rest, at: 0, take: length)
    |> result.replace_error(UnexpectedEnd)
  use slice <- result.then(slice)
  let string =
    bit_array.to_string(slice)
    |> result.replace_error(InvalidUTF8)
  use string <- result.map(string)
  BulkString(string)
}

fn parse_length(
  input: BitArray,
  length: Int,
) -> Result(#(Int, BitArray), ParseError) {
  case input, length {
    <<"\r\n":utf8, _>>, 0 -> Error(InvalidLength)
    <<"\r\n":utf8, rest:bits>>, _ -> Ok(#(length, rest))
    <<digit_code:utf8_codepoint, rest:bits>>, _ ->
      string.from_utf_codepoints([digit_code])
      |> int.parse
      |> result.map_error(fn(_) { InvalidLength })
      |> result.then(fn(digit: Int) { parse_length(rest, length * 10 + digit) })

    _, _ -> Error(InvalidLength)
  }
}
