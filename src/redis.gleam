import bravo
import bravo/uset.{type USet}
import gleam/bool
import gleam/bytes_builder
import gleam/dict
import gleam/erlang
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/iterator
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order.{Eq, Gt, Lt}
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
      let assert Ok(_) =
        case command {
          command.Ping -> resp.SimpleString("PONG")
          command.Echo(payload) -> payload

          command.Set(key: key, value: value, expiry: None) -> {
            uset.insert(table, [#(key, value.String(value), None)])
            resp.SimpleString("OK")
          }

          command.Set(key: key, value: value, expiry: Some(expiry)) -> {
            let deadline = erlang.system_time(erlang.Millisecond) + expiry
            uset.insert(table, [#(key, value.String(value), Some(deadline))])
            resp.SimpleString("OK")
          }

          command.Get(key) -> {
            case lookup(table, key) {
              value.None -> resp.Null(resp.NullString)
              value.String(s) -> resp.BulkString(s)
              _ -> todo as "can only get strings or nothing for now"
            }
          }
          command.Config(subcommand) -> {
            case subcommand {
              command.ConfigGet(parameter) -> {
                case parameter {
                  config.Dir -> config.dir
                  config.DbFilename -> config.dbfilename
                }
                |> option.map(resp.BulkString)
                |> option.unwrap(resp.Null(resp.NullString))
                |> list.wrap
                |> list.prepend(
                  resp.BulkString(config.parameter_key(parameter)),
                )
                |> resp.Array
              }
            }
          }
          command.Keys(None) -> {
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
          }
          command.Keys(_) -> todo as "KEYS command will be implemented soon"

          command.Type(key) -> {
            lookup(table, key)
            |> value.to_type_name
            |> resp.SimpleString
          }

          command.XAdd(stream, entry_id, data) -> {
            handle_xadd(table, stream, entry_id, data)
          }
        }
        |> send_resp(conn)
      actor.continue(Nil)
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

// HANDLERS

fn handle_xadd(
  table: Table,
  stream: String,
  entry_id: command.StreamEntryId,
  data: List(#(String, String)),
) -> Resp {
  case lookup(table, stream), entry_id {
    // New stream, auto id
    value.None, command.AutoGenerate -> {
      let timestamp = erlang.system_time(erlang.Millisecond)
      Ok([#(timestamp, 0, data)])
    }
    // New stream, auto sequence
    value.None, command.AutoSequence(timestamp) -> {
      let sequence = case timestamp {
        0 -> 1
        _ -> 0
      }
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: 0,
        last_seq: 0,
      )
      Ok([#(timestamp, sequence, data)])
    }
    // New stream, explicit id
    value.None, command.Explicit(timestamp, sequence) -> {
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: 0,
        last_seq: 0,
      )
      Ok([#(timestamp, sequence, data)])
    }
    // Existing stream, auto id
    value.Stream([#(last_ts, last_seq, _), ..] as entries), command.AutoGenerate
    -> {
      let time = erlang.system_time(erlang.Millisecond)
      let #(timestamp, sequence) = case int.compare(last_ts, time) {
        Lt -> #(time, 0)
        Eq | Gt -> #(last_ts, last_seq + 1)
      }
      Ok([#(timestamp, sequence, data), ..entries])
    }
    // Existing stream, auto sequence
    value.Stream([#(last_ts, last_seq, _), ..] as entries),
      command.AutoSequence(timestamp)
    -> {
      let sequence = case int.compare(last_ts, timestamp) {
        Lt | Gt -> 0
        Eq -> last_seq + 1
      }
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: last_ts,
        last_seq: last_seq,
      )
      Ok([#(timestamp, sequence, data), ..entries])
    }
    // Existing stream, explicit id
    value.Stream([#(last_ts, last_seq, _), ..] as entries),
      command.Explicit(timestamp, sequence)
    -> {
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: last_ts,
        last_seq: last_seq,
      )
      Ok([#(timestamp, sequence, data), ..entries])
    }
    _, _ -> Error(resp.Null(resp.NullString))
  }
  |> result.map(fn(entries) {
    let assert [#(timestamp, sequence, _), ..] = entries
    [#(stream, value.Stream(entries), None)]
    |> uset.insert(table, _)
    resp.stream_entry_id(timestamp, sequence)
  })
  |> result.unwrap_both
}

fn validate_entry_id(
  last_ts last_ts: Int,
  last_seq last_seq: Int,
  timestamp timestamp: Int,
  sequence sequence: Int,
  when_valid callback: fn() -> Result(a, Resp),
) {
  use <- bool.guard(
    when: timestamp < 0 || { timestamp == 0 && sequence < 1 },
    return: Error(resp.SimpleError(
      "ERR The ID specified in XADD must be greater than 0-0",
    )),
  )
  bool.guard(
    when: timestamp < last_ts
      || { timestamp == last_ts && sequence <= last_seq },
    return: Error(resp.SimpleError(
      "ERR The ID specified in XADD is equal or smaller than the target stream top item",
    )),
    otherwise: callback,
  )
}
