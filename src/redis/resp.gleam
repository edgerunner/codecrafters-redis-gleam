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

type Parse(a) =
  Result(#(a, BitArray), ParseError)

import gleam/bit_array
import gleam/int
import gleam/io
import gleam/pair
import gleam/result
import gleam/string

const crlf = <<"\r\n":utf8>>

pub fn parse(input: BitArray) -> Parse(Resp) {
  case input {
    <<"+":utf8, rest:bits>> -> parse_simple_string(rest)
    <<"$":utf8, rest:bits>> -> parse_bulk_string(rest)
    <<"*":utf8, rest:bits>> -> parse_array(rest)
    unexpected -> Error(UnexpectedInput(unexpected))
  }
}

fn parse_simple_string(input: BitArray) -> Parse(Resp) {
  case input {
    <<"\r\n":utf8, rest:bits>> -> Ok(#(SimpleString(""), rest))
    <<"\r":utf8>> | <<"\n":utf8>> | <<>> -> Error(UnexpectedEnd)
    <<"\r":utf8, _rest:bits>> | <<"\n":utf8, _rest:bits>> ->
      Error(UnexpectedInput(input))
    <<char:utf8_codepoint, rest:bits>> -> {
      use #(tail_resp, rest) <- result.map(parse_simple_string(rest))
      let assert SimpleString(tail) = tail_resp
      let head = string.from_utf_codepoints([char])
      #(SimpleString(head <> tail), rest)
    }

    unexpected -> Error(UnexpectedInput(unexpected))
  }
}

fn parse_bulk_string(input: BitArray) -> Parse(Resp) {
  use #(length, rest) <- result.then(parse_length(input, 0))
  use #(bulk, rest) <- result.then(parse_slice(rest, length))
  use #(Nil, rest) <- result.then(parse_crlf(rest))
  bit_array.to_string(bulk)
  |> result.replace_error(InvalidUTF8)
  |> result.map(BulkString)
  |> result.map(pair.new(_, rest))
}

fn parse_length(input: BitArray, length: Int) -> Parse(Int) {
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

fn parse_crlf(input: BitArray) -> Parse(Nil) {
  case input {
    <<"\r\n":utf8, rest:bits>> -> Ok(#(Nil, rest))
    <<>> -> Error(UnexpectedEnd)
    _ -> Error(UnexpectedInput(input))
  }
}

fn parse_slice(input: BitArray, length: Int) -> Parse(BitArray) {
  let slice =
    bit_array.slice(from: input, at: 0, take: length)
    |> result.replace_error(InvalidLength)
  let rest =
    bit_array.slice(
      from: input,
      at: length,
      take: bit_array.byte_size(input) - length,
    )
    |> result.replace_error(InvalidLength)
  use slice <- result.then(slice)
  use rest <- result.then(rest)
  Ok(#(slice, rest))
}

import gleam/iterator
import gleam/list

fn parse_array(input: BitArray) -> Parse(Resp) {
  use #(length, rest) <- result.then(parse_length(input, 0))
  let slots = iterator.range(from: 1, to: length)
  let array = {
    use #(array, rest), _slot <- iterator.try_fold(over: slots, from: #(
      [],
      rest,
    ))
    use #(resp, rest) <- result.map(parse(rest))

    #([resp, ..array], rest)
  }
  use #(array, rest) <- result.map(array)
  #(array |> list.reverse |> Array, rest)
}
