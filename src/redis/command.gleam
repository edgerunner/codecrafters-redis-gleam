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
  Set(key: String, value: Resp, expiry: Option(Int))
  Get(key: String)
  Config(ConfigSubcommand)
  Keys(Option(String))
  Type(String)
  XAdd(stream: String, entry: String, data: List(#(String, Resp)))
}

pub type ConfigSubcommand {
  ConfigGet(config.Parameter)
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

    "KEYS", [BulkString("*")] -> Ok(Keys(option.None))
    "KEYS", [BulkString(_)] -> todo
    "KEYS", _ -> Error(WrongNumberOfArguments)

    "TYPE", [key] ->
      resp.to_string(key)
      |> result.replace_error(InvalidArgument)
      |> result.map(Type)
    "TYPE", _ -> Error(WrongNumberOfArguments)

    "XADD", [stream, entry, ..data] -> {
      let as_string = fn(resp: Resp, callback: fn(String) -> Result(a, Error)) {
        resp.to_string(resp)
        |> result.replace_error(InvalidArgument)
        |> result.then(callback)
      }
      use stream <- as_string(stream)
      use entry <- as_string(entry)

      let data =
        list.sized_chunk(in: data, into: 2)
        |> list.try_map(fn(kv2) {
          let assert [key, val] = kv2
          use key <- as_string(key)
          Ok(#(key, val))
        })

      use data <- result.then(data)

      Ok(XAdd(stream, entry, data))
    }

    unknown, _ -> Error(UnknownCommand(unknown))
  }
}

fn parse_set(args: List(Resp)) -> Result(Command, Error) {
  case args {
    [BulkString(key), value] ->
      Ok(Set(key: key, value: value, expiry: option.None))
    [BulkString(key), value, BulkString(expiry), BulkString(duration)] ->
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

fn with_command(
  from resp: List(Resp),
  with fun: fn(String, List(Resp)) -> Result(Command, Error),
) -> Result(Command, Error) {
  let command =
    list.first(resp)
    |> result.then(resp.to_string)
    |> result.map(string.uppercase)
    |> result.replace_error(InvalidCommand)

  let args = list.rest(resp) |> result.replace_error(InvalidCommand)

  use command <- result.then(command)
  use args <- result.then(args)
  fun(command, args)
}
