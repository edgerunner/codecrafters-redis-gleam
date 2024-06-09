import argv
import gleam/list
import gleam/option.{type Option, None, Some}

pub type Config {
  Config(dir: Option(String), dbfilename: Option(String))
}

pub type Error {
  ExpectingDir(Config)
  ExpectingDBFilename(Config)
  UnexpectedState
}

pub const default = Config(dir: None, dbfilename: None)

pub fn load() -> Result(Config, Error) {
  let args = argv.load().arguments
  use config, arg <- list.fold(args, Ok(default))
  case config, arg {
    Ok(config), "--dir" -> config |> ExpectingDir |> Error
    Error(ExpectingDir(config)), dir -> Ok(Config(..config, dir: Some(dir)))
    Ok(config), "--dbfilename" -> config |> ExpectingDBFilename |> Error
    Error(ExpectingDBFilename(config)), dbfilename ->
      Ok(Config(..config, dbfilename: Some(dbfilename)))
    Ok(config), _ -> Ok(config)
    _, _ -> Error(UnexpectedState)
  }
}
