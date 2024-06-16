import argv
import gleam/list
import gleam/option.{type Option, None, Some}

pub type Config {
  Config(dir: Option(String), dbfilename: Option(String))
}

pub type Parameter {
  Dir
  DbFilename
}

pub const default = Config(dir: None, dbfilename: None)

pub fn load() -> Config {
  let args = list.window_by_2(argv.load().arguments)
  use config, #(left, right) <- list.fold(args, default)
  case left, right {
    "--dir", dir -> Config(..config, dir: Some(dir))
    "--dbfilename", dbfilename -> Config(..config, dbfilename: Some(dbfilename))
    _, _ -> config
  }
}

pub fn parameter_key(parameter: Parameter) -> String {
  case parameter {
    Dir -> "dir"
    DbFilename -> "dbfilename"
  }
}

pub fn db_full_path(config: Config) -> Option(String) {
  use dir <- option.then(config.dir)
  use dbfilename <- option.map(config.dbfilename)
  dir <> "/" <> dbfilename
}
