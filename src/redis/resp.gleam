pub type Resp {
  SimpleString(String)
  BulkString(String)
  BulkData(BitArray)
  Array(List(Resp))
  Null(NullType)
  SimpleError(String)
  Integer(Int)
}

pub type NullType {
  NullPrimitive
  NullString
  NullArray
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
import gleam/iterator.{type Iterator}
import gleam/list
import gleam/result
import gleam/string

const crlf = <<"\r\n":utf8>>

pub fn iterate(input: BitArray) -> Iterator(#(Resp, Int)) {
  use bits <- iterator.unfold(from: input)
  case parse(bits) {
    Ok(#(resp, rest)) -> {
      let offset = bit_array.byte_size(bits) - bit_array.byte_size(rest)
      iterator.Next(#(resp, offset), rest)
    }
    Error(_) -> iterator.Done
  }
}

pub fn parse(input: BitArray) -> Parse(Resp) {
  case input {
    <<"_":utf8, rest:bits>> -> parse_null(NullPrimitive, rest)
    <<"+":utf8, rest:bits>> -> parse_simple(rest) |> map(SimpleString)
    <<"-":utf8, rest:bits>> -> parse_simple(rest) |> map(SimpleError)
    <<"$-1":utf8, rest:bits>> -> parse_null(NullString, rest)
    <<"$":utf8, rest:bits>> -> parse_bulk_string(rest)
    <<"*-1":utf8, rest:bits>> -> parse_null(NullArray, rest)
    <<"*":utf8, rest:bits>> -> parse_array(rest)
    <<":":utf8, rest:bits>> ->
      parse_simple(rest) |> then(int.parse) |> map(Integer)
    unexpected -> Error(UnexpectedInput(unexpected))
  }
}

fn parse_null(null_type: NullType, input: BitArray) -> Parse(Resp) {
  use #(_, rest) <- result.map(parse_crlf(input))
  #(Null(null_type), rest)
}

fn map(parse: Parse(a), with mapper: fn(a) -> b) -> Parse(b) {
  use #(a, rest) <- result.map(parse)
  #(mapper(a), rest)
}

fn then(parse: Parse(a), with mapper: fn(a) -> Result(b, e)) -> Parse(b) {
  use #(a, rest) <- result.then(parse)
  case mapper(a) {
    Ok(b) -> Ok(#(b, rest))
    Error(_) -> Error(ParseError)
  }
}

fn parse_simple(input: BitArray) -> Parse(String) {
  case input {
    <<"\r\n":utf8, rest:bits>> -> Ok(#("", rest))
    <<"\r":utf8>> | <<"\n":utf8>> | <<>> -> Error(UnexpectedEnd)
    <<"\r":utf8, _rest:bits>> | <<"\n":utf8, _rest:bits>> ->
      Error(UnexpectedInput(input))
    <<char:utf8_codepoint, rest:bits>> -> {
      use #(tail, rest) <- result.map(parse_simple(rest))

      let head = string.from_utf_codepoints([char])
      #(head <> tail, rest)
    }

    unexpected -> Error(UnexpectedInput(unexpected))
  }
}

fn parse_bulk_string(input: BitArray) -> Parse(Resp) {
  use #(length, rest) <- result.then(parse_length(input, 0))
  use #(bulk, rest) <- result.then(parse_slice(rest, length))

  case bit_array.to_string(bulk) {
    Ok(string) -> {
      use #(Nil, rest) <- result.map(parse_crlf(rest))
      #(BulkString(string), rest)
    }
    Error(Nil) -> Ok(#(BulkData(bulk), rest))
  }
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

fn parse_array(input: BitArray) -> Parse(Resp) {
  use #(length, rest) <- result.then(parse_length(input, 0))
  let slots = iterator.range(from: 1, to: length)
  let array = {
    use #(array, rest), _slot <- iterator.try_fold(over: slots, from: #(
      [],
      rest,
    ))

    case parse(rest) {
      Ok(#(resp, rest)) -> Ok(#([resp, ..array], rest))
      Error(UnexpectedInput(<<>>)) -> Error(InvalidLength)
      Error(e) -> Error(e)
    }
  }
  use #(array, rest) <- result.map(array)
  #(array |> list.reverse |> Array, rest)
}

pub fn encode(resp: Resp) -> BitArray {
  case resp {
    Null(n) -> encode_null(n)
    SimpleString(s) -> encode_simple_string(s)
    SimpleError(e) -> encode_simple_error(e)
    BulkString(s) -> encode_bulk_string(s)
    BulkData(b) -> encode_bulk_data(b)
    Array(a) -> encode_array(a)
    Integer(i) -> encode_integer(i)
  }
}

fn encode_null(null_type: NullType) -> BitArray {
  case null_type {
    NullPrimitive -> <<"_":utf8, crlf:bits>>
    NullString -> <<"$-1":utf8, crlf:bits>>
    NullArray -> <<"*-1":utf8, crlf:bits>>
  }
}

fn encode_simple_string(string: String) -> BitArray {
  <<"+":utf8, string:utf8, crlf:bits>>
}

fn encode_simple_error(string: String) -> BitArray {
  <<"-":utf8, string:utf8, crlf:bits>>
}

fn encode_bulk_string(string: String) -> BitArray {
  let length = encode_length(string.length(string))
  <<"$":utf8, length:bits, string:utf8, crlf:bits>>
}

fn encode_bulk_data(bits: BitArray) -> BitArray {
  let length = encode_length(bit_array.byte_size(bits))
  <<"$":utf8, length:bits, bits:bits>>
}

fn encode_length(length: Int) -> BitArray {
  <<int.to_string(length):utf8, crlf:bits>>
}

fn encode_array(array: List(Resp)) -> BitArray {
  let length = encode_length(list.length(array))
  let preamble = <<"*":utf8, length:bits>>
  use buffer, resp <- list.fold(over: array, from: preamble)
  <<buffer:bits, encode(resp):bits>>
}

fn encode_integer(integer: Int) -> BitArray {
  <<":":utf8, int.to_string(integer):utf8, crlf:bits>>
}

pub fn to_string(resp: Resp) -> Result(String, Nil) {
  case resp {
    BulkString(bs) -> Ok(bs)
    SimpleString(ss) -> Ok(ss)
    _ -> Error(Nil)
  }
}

pub fn to_list(resp: Resp) -> List(Resp) {
  case resp {
    SimpleString(_) | BulkString(_) | SimpleError(_) | BulkData(_) | Integer(_) -> [
      resp,
    ]
    Array(list) -> list
    Null(_) -> []
  }
}

pub fn array_header(length: Int) -> BitArray {
  <<"*":utf8, encode_length(length):bits>>
}
