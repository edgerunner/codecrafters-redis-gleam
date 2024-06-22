import argv
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/pair
import gleam/result
import gleam/string

pub type Config {
  Config(
    dir: Option(String),
    dbfilename: Option(String),
    port: Int,
    replicaof: Option(#(String, Int)),
  )
}

pub type Parameter {
  Dir
  DbFilename
  Port
  ReplicaOf
}

const default_port = 6379

pub const default = Config(
  dir: None,
  dbfilename: None,
  port: default_port,
  replicaof: None,
)

pub fn load() -> Config {
  let args = list.window_by_2(argv.load().arguments)
  use config, #(left, right) <- list.fold(args, default)
  case left, right {
    "--dir", dir -> Config(..config, dir: Some(dir))
    "--dbfilename", dbfilename -> Config(..config, dbfilename: Some(dbfilename))
    "--port", port ->
      Config(..config, port: port |> int.parse |> result.unwrap(config.port))

    "--replicaof", master -> {
      let replicaof = case string.split_once(master, " ") {
        Ok(#(host, port)) ->
          int.parse(port) |> result.map(pair.new(host, _)) |> option.from_result
        Error(_) -> Some(#(master, default_port))
      }
      Config(..config, replicaof: replicaof)
    }

    _, _ -> config
  }
}

pub fn parameter_key(parameter: Parameter) -> String {
  case parameter {
    Dir -> "dir"
    DbFilename -> "dbfilename"
    Port -> "port"
    ReplicaOf -> "replicaof"
  }
}

pub fn db_full_path(config: Config) -> Option(String) {
  use dir <- option.then(config.dir)
  use dbfilename <- option.map(config.dbfilename)
  dir <> "/" <> dbfilename
}

pub fn to_string(config: Config, parameter: Parameter) -> Option(String) {
  case parameter, config {
    Dir, Config(dir: dir, ..) -> dir
    DbFilename, Config(dbfilename: dbfilename, ..) -> dbfilename
    Port, Config(port: port, ..) -> int.to_string(port) |> Some
    ReplicaOf, Config(replicaof: Some(#(host, port)), ..) ->
      Some(host <> " " <> int.to_string(port))
    ReplicaOf, Config(replicaof: None, ..) -> None
  }
}
