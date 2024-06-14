import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/iterator.{type Iterator}
import gleam/option.{type Option}
import gleam/result
import gleam/string
import redis/resp.{type Resp}

pub type RDB {
  RDB(
    version: String,
    metadata: Dict(String, String),
    databases: Dict(Int, Database),
  )
}

type ValueType {
  StringValue
}

type Database =
  Dict(String, Row)

type Row =
  #(String, Resp, Option(Int))

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

  use #(metadata, data) <- result.then(metadata_parse_result)

  let databases_parse_result =
    collect(from: data, with: parse_database_with_id)
    |> into_dict

  use #(databases, _data) <- result.then(databases_parse_result)

  Ok(RDB(version, metadata, databases))
}

fn parse_string(from data: BitArray) -> Parsed(String) {
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

fn parse_size(from data: BitArray) -> Parsed(Size) {
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

fn parse_symbol(from data: BitArray, match symbol: BitArray) -> Parsed(Nil) {
  let bit_size = bit_array.byte_size(symbol) * 8
  case data {
    <<match:bits-size(bit_size), rest:bits>> if match == symbol ->
      Ok(#(Nil, rest))
    _ -> Error("Symbol " <> bit_array.inspect(symbol) <> "not found")
  }
}

fn discard_resizedb(from data: BitArray) -> Parsed(Nil) {
  // discard resizedb opcode
  use #(_, data) <- result.then(parse_symbol(from: data, match: <<0xFB>>))
  // discard resizedb key-value hash size
  use #(_, data) <- result.then(parse_size(data))
  // discard resizedb expiry hash size
  use #(_, data) <- result.then(parse_size(data))
  Ok(#(Nil, data))
}

fn parse_database_with_id(from data: BitArray) -> Parsed(#(Int, Database)) {
  use #(_, data) <- result.then(parse_symbol(from: data, match: <<0xFE>>))
  use #(db_id, data) <- result.then(parse_db_id(from: data))
  use #(_, data) <- result.then(discard_resizedb(from: data))
  use #(key_values, data) <- result.then(parse_database(from: data))
  Ok(#(#(db_id, key_values), data))
}

fn parse_db_id(from data: BitArray) -> Parsed(Int) {
  use #(s, data) <- result.then(parse_size(from: data))
  case s {
    Length(l) -> Ok(#(l, data))
    Integer(i) -> Ok(#(i, data))
  }
}

fn parse_database(from data: BitArray) -> Parsed(Database) {
  let extract_key = fn(item) {
    use #(key, _, _) as row <- result.map(item)
    #(key, row)
  }

  collect(from: data, with: parse_row)
  |> iterator.map(with: extract_key)
  |> into_dict
}

fn parse_row(from data: BitArray) -> Parsed(Row) {
  use #(datatype, data) <- result.then(parse_value_type(data))
  use #(key, data) <- result.then(parse_string(data))
  case datatype {
    StringValue -> {
      use #(value, data) <- result.then(parse_string(data))
      Ok(#(#(key, resp.BulkString(value), option.None), data))
    }
  }
}

fn parse_value_type(from data: BitArray) -> Parsed(ValueType) {
  case data {
    <<0x00, rest:bits>> -> Ok(#(StringValue, rest))
    _ -> Error("Only string values are supported for now")
  }
}

fn collect(
  from data: BitArray,
  with parser: fn(BitArray) -> Parsed(a),
) -> Iterator(Result(a, BitArray)) {
  case parser(data) {
    Ok(#(a, data)) -> {
      use <- iterator.yield(Ok(a))
      collect(from: data, with: parser)
    }
    Error(_) -> iterator.single(Error(data))
  }
}

fn into_dict(
  from iterator: Iterator(Result(#(key, value), BitArray)),
) -> Parsed(Dict(key, value)) {
  use acc, entry <- iterator.fold(over: iterator, from: Ok(#(dict.new(), <<>>)))
  let assert Ok(#(dict, _)) = acc
  case entry {
    Ok(#(key, value)) -> #(dict.insert(dict, key, value), <<>>) |> Ok
    Error(data) -> Ok(#(dict, data))
  }
}
