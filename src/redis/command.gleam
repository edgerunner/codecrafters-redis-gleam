import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import redis/config
import redis/resp.{type Resp, Array, BulkString, Null, SimpleString}

pub type Command {
  Ping
  Echo(Resp)
  Set(key: String, value: String, expiry: Option(Int))
  Get(key: String)
  Config(ConfigSubcommand)
  Keys(Option(String))
  Type(String)
  XAdd(stream: String, id: StreamEntryId, data: List(#(String, String)))
  XRange(stream: String, start: StreamEntryId, end: StreamEntryId)
  XRead(streams: List(#(String, StreamEntryId)), block: Option(Int))
  Info(InfoSubcommand)
}

pub type ConfigSubcommand {
  ConfigGet(config.Parameter)
}

pub type InfoSubcommand {
  InfoReplication
}

pub type StreamEntryId {
  Unspecified
  Timestamp(timestamp: Int)
  Explicit(timestamp: Int, sequence: Int)
}

pub type Error {
  InvalidCommand
  InvalidSubcommand
  WrongNumberOfArguments
  UnknownCommand(String)
  InvalidArgument
  UnknownFlag(String)
}

pub fn parse(resp: Resp) -> Result(Command, Error) {
  case resp {
    SimpleString(_) | BulkString(_) -> parse_list([resp])
    Array(list) -> parse_list(list)
    Null(_) -> Error(InvalidCommand)
    resp.SimpleError(e) -> Error(UnknownCommand(e))
  }
}

fn parse_list(list: List(Resp)) -> Result(Command, Error) {
  use command, args <- with_command(from: list)
  case command, args {
    "PING", [] -> Ok(Ping)
    "PING", _ -> Error(WrongNumberOfArguments)

    "ECHO", [payload] -> Ok(Echo(payload))
    "ECHO", _ -> Error(WrongNumberOfArguments)

    "SET", _ -> parse_set(args)

    "GET", [key] ->
      resp.to_string(key)
      |> result.replace_error(InvalidArgument)
      |> result.map(Get)
    "GET", _ -> Error(WrongNumberOfArguments)

    "CONFIG", _ -> parse_config(args)
    "INFO", _ -> parse_info(args)

    "KEYS", [BulkString("*")] -> Ok(Keys(option.None))
    "KEYS", [BulkString(_)] ->
      Error(UnknownFlag("KEYS command matchers will be implemented soon"))
    "KEYS", _ -> Error(WrongNumberOfArguments)

    "TYPE", [key] ->
      resp.to_string(key)
      |> result.replace_error(InvalidArgument)
      |> result.map(Type)
    "TYPE", _ -> Error(WrongNumberOfArguments)

    "XADD", [stream, entry, ..data] -> {
      use stream <- as_string(stream)
      use entry <- as_xadd_entry_id(entry)

      let data =
        list.sized_chunk(in: data, into: 2)
        |> list.try_map(fn(kv2) {
          let assert [key, val] = kv2
          use key <- as_string(key)
          use val <- as_string(val)
          Ok(#(key, val))
        })

      use data <- result.then(data)

      Ok(XAdd(stream, entry, data))
    }

    "XRANGE", [stream, start, end] -> {
      use stream <- as_string(stream)
      use start <- as_xrange_entry_id(start, "-")
      use end <- as_xrange_entry_id(end, "+")
      Ok(XRange(stream, start, end))
    }
    "XRANGE", _ -> Error(WrongNumberOfArguments)

    "XREAD", args -> parse_xread(args, option.None)

    unknown, _ -> Error(UnknownCommand(unknown))
  }
}

fn parse_set(args: List(Resp)) -> Result(Command, Error) {
  case args {
    [BulkString(key), BulkString(value)] ->
      Ok(Set(key: key, value: value, expiry: option.None))
    [
      BulkString(key),
      BulkString(value),
      BulkString(expiry),
      BulkString(duration),
    ] ->
      case string.uppercase(expiry), int.parse(duration) {
        "PX", Ok(duration) -> Ok(duration)
        "EX", Ok(duration) -> Ok(duration * 1000)
        "PX", _ | "EX", _ -> Error(InvalidArgument)
        unknown, _ -> Error(UnknownFlag(unknown))
      }
      |> result.map(option.Some)
      |> result.map(Set(key: key, value: value, expiry: _))
    _ -> Error(WrongNumberOfArguments)
  }
}

fn parse_config(args: List(Resp)) -> Result(Command, Error) {
  use subcommand, args <- with_command(from: args)
  case subcommand, args {
    "GET", [BulkString(key)] ->
      case key {
        "dir" -> Ok(Config(ConfigGet(config.Dir)))
        "dbfilename" -> Ok(Config(ConfigGet(config.DbFilename)))
        _ -> Error(InvalidArgument)
      }
    "GET", _ -> Error(InvalidArgument)
    unknown, _ -> Error(UnknownCommand(unknown))
  }
}

fn parse_info(args: List(Resp)) -> Result(Command, Error) {
  use subcommand, args <- with_command(from: args)
  case subcommand, args {
    "REPLICATION", [] -> Ok(Info(InfoReplication))
    _, _ -> Error(InvalidSubcommand)
  }
}

fn parse_xread(args: List(Resp), block: Option(Int)) -> Result(Command, Error) {
  use subcommand, args <- with_command(from: args)
  case subcommand {
    "STREAMS" -> {
      let #(keys, ids) =
        list.length(args) / 2
        |> list.split(args, _)
      use keys <- as_xread_entry_keys(keys)
      use ids <- as_xread_entry_ids(ids)
      list.strict_zip(keys, ids)
      |> result.replace_error(InvalidArgument)
      |> result.map(XRead(_, block))
    }
    "BLOCK" -> {
      use duration <- then(list.first(args), or: WrongNumberOfArguments)
      use duration <- as_string(duration)
      use duration <- as_int(duration)
      use rest <- then(list.rest(args), or: WrongNumberOfArguments)
      parse_xread(rest, option.Some(duration))
    }
    _ -> Error(InvalidSubcommand)
  }
}

fn with_command(
  from resp: List(Resp),
  with fun: fn(String, List(Resp)) -> Result(Command, Error),
) -> Result(Command, Error) {
  let command =
    list.first(resp)
    |> result.then(resp.to_string)
    |> result.map(string.uppercase)

  use command <- then(either: command, or: InvalidCommand)
  use args <- then(either: list.rest(resp), or: InvalidCommand)
  fun(command, args)
}

fn as_xadd_entry_id(resp: Resp, callback: fn(StreamEntryId) -> Result(a, Error)) {
  use id_string <- as_string(resp)
  case id_string, string.split_once(id_string, on: "-") {
    "*", _ -> callback(Unspecified)
    _, Ok(#(timestamp, "*")) -> {
      use timestamp <- as_int(timestamp)
      Timestamp(timestamp) |> callback
    }
    _, Ok(#(timestamp, sequence)) -> {
      use timestamp <- as_int(timestamp)
      use sequence <- as_int(sequence)
      Explicit(timestamp, sequence)
      |> callback
    }
    _, _ -> Error(InvalidArgument)
  }
}

fn as_xrange_entry_id(
  resp: Resp,
  wildcard: String,
  callback: fn(StreamEntryId) -> Result(a, Error),
) -> Result(a, Error) {
  use id_string <- as_string(resp)
  case id_string, string.split_once(id_string, on: "-") {
    w, _ if w == wildcard -> Unspecified |> callback
    t, Error(_) -> {
      use timestamp <- as_int(t)
      Timestamp(timestamp) |> callback
    }
    _, Ok(#(timestamp, sequence)) -> {
      use timestamp <- as_int(timestamp)
      use sequence <- as_int(sequence)
      Explicit(timestamp, sequence) |> callback
    }
  }
}

fn as_xread_entry_keys(
  resps: List(Resp),
  callback: fn(List(String)) -> Result(a, Error),
) -> Result(a, Error) {
  as_string(_, Ok)
  |> list.try_map(resps, _)
  |> result.then(callback)
}

fn as_xread_entry_ids(
  resps: List(Resp),
  callback: fn(List(StreamEntryId)) -> Result(a, Error),
) -> Result(a, Error) {
  as_xrange_entry_id(_, "$", Ok)
  |> list.try_map(resps, _)
  |> result.then(callback)
}

fn as_string(resp: Resp, callback: fn(String) -> Result(a, Error)) {
  then(either: resp.to_string(resp), with: callback, or: InvalidArgument)
}

fn as_int(string: String, callback: fn(Int) -> Result(a, Error)) {
  then(either: int.parse(string), with: callback, or: InvalidArgument)
}

fn then(
  either result: Result(a, e),
  or error: f,
  with callback: fn(a) -> Result(b, f),
) -> Result(b, f) {
  result
  |> result.replace_error(error)
  |> result.then(callback)
}
