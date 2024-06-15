import bravo
import bravo/uset.{type USet}
import gleam/bytes_builder
import gleam/dict
import gleam/erlang
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/iterator
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/otp/actor
import gleam/result
import glisten.{type Connection, type Message, Packet, User}
import redis/command
import redis/config.{type Config}
import redis/rdb
import redis/resp.{type Resp}
import redis/value.{type RedisValue}
import simplifile

type Row =
  #(String, RedisValue, Option(Int))

type Table =
  USet(Row)

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
          uset.insert(table, [#(key, value.String(value), None)])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(Nil)
        }

        command.Set(key: key, value: value, expiry: Some(expiry)) -> {
          let deadline = erlang.system_time(erlang.Millisecond) + expiry
          uset.insert(table, [#(key, value.String(value), Some(deadline))])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(Nil)
        }

        command.Get(key) -> {
          let assert Ok(_) =
            case lookup(table, key) {
              value.None -> resp.Null(resp.NullString)
              value.String(s) -> resp.BulkString(s)
              _ -> todo as "can only get strings or nothing for now"
            }
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
            lookup(table, key)
            |> value.to_type_name
            |> resp.SimpleString
            |> send_resp(conn)
          actor.continue(Nil)
        }

        command.XAdd(stream, entry_id, data) -> {
          let #(timestamp, sequence) = case entry_id {
            command.AutoGenerate -> #(erlang.system_time(erlang.Millisecond), 0)
            command.AutoSequence(timestamp) -> #(timestamp, 0)
            command.Explicit(timestamp, sequence) -> #(timestamp, sequence)
          }
          let validate = fn(last_ts, last_seq) {
            case
              int.compare(last_ts, timestamp),
              int.compare(last_seq, sequence)
            {
              order.Lt, _ | order.Eq, order.Gt -> #(timestamp, sequence)
              _, _ -> #(last_ts, int.max(last_seq + 1, sequence))
            }
          }
          let assert Ok(_) =
            case lookup(table, stream) {
              value.None -> {
                let #(timestamp, sequence) = validate(0, 0)
                [#(stream, value.Stream([#(timestamp, sequence, data)]), None)]
                |> uset.insert(table, _)
                resp.SimpleString(
                  int.to_string(timestamp) <> "-" <> int.to_string(sequence),
                )
              }
              value.Stream([#(last_ts, last_seq, _), ..] as entries) -> {
                let #(timestamp, sequence) = validate(last_ts, last_seq)
                [
                  #(
                    stream,
                    value.Stream([#(timestamp, sequence, data), ..entries]),
                    None,
                  ),
                ]
                |> uset.insert(table, _)
                resp.SimpleString(
                  int.to_string(timestamp) <> "-" <> int.to_string(sequence),
                )
              }
              _ -> resp.Null(resp.NullString)
            }
            |> send_resp(conn)
          actor.continue(Nil)
        }
      }
    }
    User(_) -> actor.continue(Nil)
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

fn lookup(table: Table, key: String) -> RedisValue {
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

const store_name = "redis_on_ets"

fn load_rdb(table: USet(Row), config: Config) {
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
