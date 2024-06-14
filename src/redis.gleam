import bravo
import bravo/uset.{type USet}
import gleam/bytes_builder
import gleam/dict
import gleam/erlang
import gleam/erlang/process
import gleam/io
import gleam/iterator
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import glisten.{type Connection, type Message, Packet, User}
import redis/command
import redis/config.{type Config}
import redis/rdb
import redis/resp.{type Resp}
import simplifile

type Value =
  #(String, Resp, Option(Int))

type Table =
  USet(Value)

pub fn main() {
  let config = config.load()
  let assert Ok(table) = uset.new(store_name, 1, bravo.Public)
  load_rdb(table, config)

  let assert Ok(_) =
    glisten.handler(fn(_) { #(Nil, None) }, fn(msg, _state, conn) {
      router(msg, table, config, conn)
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn router(msg: Message(a), table: Table, config: Config, conn: Connection(a)) {
  case msg {
    Packet(resp_binary) -> {
      let assert Ok(#(resp, _)) = resp_binary |> resp.parse

      let assert Ok(command) = command.parse(resp)
      case command {
        command.Ping -> {
          let assert Ok(_) =
            resp.SimpleString("PONG")
            |> send_resp(conn)

          actor.continue(Nil)
        }
        command.Echo(payload) -> {
          let assert Ok(_) = send_resp(payload, conn)
          actor.continue(Nil)
        }

        command.Set(key: key, value: value, expiry: None) -> {
          uset.insert(table, [#(key, value, None)])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(Nil)
        }

        command.Set(key: key, value: value, expiry: Some(expiry)) -> {
          let deadline = erlang.system_time(erlang.Millisecond) + expiry
          uset.insert(table, [#(key, value, Some(deadline))])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(Nil)
        }

        command.Get(key) -> {
          let assert Ok(_) =
            lookup(table, key)
            |> send_resp(conn)

          actor.continue(Nil)
        }
        command.Config(subcommand) -> {
          let assert Ok(_) = case subcommand {
            command.ConfigGet(parameter) -> {
              case parameter {
                config.Dir -> config.dir
                config.DbFilename -> config.dbfilename
              }
              |> option.map(resp.BulkString)
              |> option.unwrap(resp.Null(resp.NullString))
              |> list.wrap
              |> list.prepend(resp.BulkString(config.parameter_key(parameter)))
              |> resp.Array
              |> send_resp(conn)
            }
          }

          actor.continue(Nil)
        }
        command.Keys(None) -> {
          let assert Ok(_) =
            {
              use key <- iterator.unfold(from: uset.first(table))

              case key {
                Error(Nil) -> iterator.Done
                Ok(key) -> iterator.Next(key, uset.next(table, key))
              }
            }
            |> iterator.map(resp.BulkString)
            |> iterator.to_list
            |> resp.Array
            |> send_resp(conn)
          actor.continue(Nil)
        }
        command.Keys(_) -> todo as "KEYS command will be implemented soon"

        command.Type(key) -> {
          let assert Ok(_) =
            case lookup(table, key) {
              resp.BulkString(_) -> "string"
              resp.Null(_) -> "none"
              _ -> ""
            }
            |> resp.SimpleString
            |> send_resp(conn)
          actor.continue(Nil)
        }
      }
    }
    User(_) -> todo
  }
}

fn send_resp(
  resp: Resp,
  conn: Connection(a),
) -> Result(Nil, glisten.SocketReason) {
  resp.encode(resp)
  |> bytes_builder.from_bit_array
  |> glisten.send(conn, _)
}

fn lookup(table: Table, key: String) -> Resp {
  let posix = erlang.system_time(erlang.Millisecond)
  case uset.lookup(table, key) {
    Error(Nil) -> resp.Null(resp.NullString)
    Ok(#(_, value, None)) -> value
    Ok(#(_, value, Some(deadline))) if deadline > posix -> value
    Ok(#(key, _, _)) -> {
      uset.delete_key(table, key)
      resp.Null(resp.NullString)
    }
  }
}

const store_name = "redis_on_ets"

fn load_rdb(table: USet(Value), config: Config) {
  use dir <- option.then(config.dir)
  use dbfilename <- option.then(config.dbfilename)
  let fullpath = dir <> "/" <> dbfilename
  io.println("Reading RDB file at " <> fullpath)

  simplifile.read_bits(from: fullpath)
  |> result.replace_error("Could not read RDB file")
  |> result.then(rdb.parse)
  |> result.then(fn(rdb) {
    dict.get(rdb.databases, 0) |> result.replace_error("database 0 not found")
  })
  |> result.map(dict.values)
  |> result.map(uset.insert(table, _))
  |> result.unwrap(or: False)

  None
}
