import gleam/bit_array
import gleam/option.{type Option}
import gleam/result
import gleam/string
import redis/resp.{type Resp}

pub type RDB {
  RDB(
    version: String,
    metadata: List(#(String, String)),
    database: List(#(String, Resp, Option(Int))),
  )
}

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
  use #(version, _rest) <- result.then(header)

  Ok(RDB(version, [], []))
}
