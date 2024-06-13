import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/iterator
import gleam/option.{type Option}
import gleam/result
import gleam/string
import redis/resp.{type Resp}

pub type RDB {
  RDB(
    version: String,
    metadata: Dict(String, String),
    database: List(#(String, Resp, Option(Int))),
  )
}

type Parsed(a) =
  Result(#(a, BitArray), String)

pub fn parse(data: BitArray) -> Result(RDB, String) {
  let header = case data {
    <<
      "REDIS":utf8,
      v0:utf8_codepoint,
      v1:utf8_codepoint,
      v2:utf8_codepoint,
      v3:utf8_codepoint,
      rest:bits,
    >> -> Ok(#(string.from_utf_codepoints([v0, v1, v2, v3]), rest))
    _ -> Error("Failed at the header")
  }
  use #(version, data) <- result.then(header)

  let metadata_parse_result =
    {
      use data <- iterator.unfold(from: data)
      case data {
        <<0xFA, data:bits>> ->
          {
            use #(key, data) <- result.then(parse_string(data))
            use #(value, data) <- result.map(parse_string(data))
            iterator.Next(element: Ok(#(key, value, data)), accumulator: data)
          }
          |> result.map_error(with: fn(e) {
            iterator.Next(element: Error(e), accumulator: <<>>)
          })
          |> result.unwrap_both

        _ -> iterator.Done
      }
    }
    |> iterator.try_fold(from: #(dict.new(), <<>>), with: fn(acc, elem) {
      let #(dict, _) = acc
      use #(key, value, data) <- result.map(over: elem)
      #(dict.insert(insert: value, into: dict, for: key), data)
    })

  use #(metadata, _data) <- result.then(metadata_parse_result)

  Ok(RDB(version, metadata, []))
}

fn parse_string(data: BitArray) -> Parsed(String) {
  use #(size, data) <- result.then(parse_size(data))
  case size {
    Length(l) -> {
      case data {
        <<string_bytes:bytes-size(l), rest:bits>> ->
          bit_array.to_string(string_bytes)
          |> result.map(fn(string) { #(string, rest) })
          |> result.replace_error(
            "Invalid UTF-8 for length " <> int.to_string(l),
          )
        _ -> Error("Not enough data for the parsed size " <> int.to_string(l))
      }
    }

    Integer(bits) -> {
      case data {
        <<integer:size(bits)-unsigned-little, rest:bits>> ->
          #(int.to_string(integer), rest) |> Ok

        _ -> Error("Failed to parse integer")
      }
    }
  }
}

type Size {
  Length(Int)
  Integer(Int)
}

fn parse_size(data: BitArray) -> Parsed(Size) {
  case data {
    <<0b00:2, size:unsigned-6, rest:bits>> -> Ok(#(Length(size), rest))
    <<0b01:2, size:unsigned-big-14, rest:bits>> -> Ok(#(Length(size), rest))
    <<0b10:2, _ignore:6, size:unsigned-big-32, rest:bits>> ->
      Ok(#(Length(size), rest))
    <<0xC0, rest:bits>> -> Ok(#(Integer(8), rest))
    <<0xC1, rest:bits>> -> Ok(#(Integer(16), rest))
    <<0xC2, rest:bits>> -> Ok(#(Integer(32), rest))
    <<0xC3, _rest:bits>> -> Error("LZF compression isn't supported")

    _ -> Error("Invalid RDB size")
  }
}
