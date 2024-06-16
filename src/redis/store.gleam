import bravo.{Public}
import bravo/uset.{type USet}
import gleam/dict
import gleam/erlang
import gleam/io
import gleam/iterator.{type Iterator}
import gleam/option.{type Option, None, Some}
import gleam/result
import redis/rdb
import redis/value.{type RedisValue}
import simplifile

pub type Row =
  #(String, RedisValue, Option(Int))

pub type Table =
  USet(Row)

pub fn lookup(table: Table, key: String) -> RedisValue {
  let posix = erlang.system_time(erlang.Millisecond)
  case uset.lookup(table, key) {
    Error(Nil) -> value.None
    Ok(#(_, value, None)) -> value
    Ok(#(_, value, Some(deadline))) if deadline > posix -> value
    Ok(#(key, _, _)) -> {
      uset.delete_key(table, key)
      value.None
    }
  }
}

pub fn insert(
  into table: Table,
  key key: String,
  value value: RedisValue,
  deadline deadline: Option(Int),
) -> Bool {
  uset.insert(table, [#(key, value, deadline)])
}

pub const name = "redis_on_ets"

pub fn new() -> Result(Table, String) {
  uset.new(name: name, keypos: 1, access: Public)
  |> result.replace_error("Failed to initialize table")
}

pub fn keys(table: Table) -> Iterator(String) {
  use key <- iterator.unfold(from: uset.first(table))

  case key {
    Error(Nil) -> iterator.Done
    Ok(key) -> iterator.Next(key, uset.next(table, key))
  }
}

pub fn load(fullpath: String) -> Result(Table, String) {
  use table <- result.then(new())

  io.println("Reading RDB file at " <> fullpath)
  case simplifile.read_bits(from: fullpath) {
    Error(simplifile.Enoent) -> Ok(table)
    Error(_) -> Error("Could not read RDB file")
    Ok(rdb) -> {
      rdb.parse(rdb)
      |> result.then(fn(rdb) {
        dict.get(rdb.databases, 0)
        |> result.map(dict.values)
        |> result.replace_error("database 0 not found")
      })
      |> result.map(uset.insert(table, _))
      |> result.replace(table)
    }
  }
}
