import gleam/bytes_builder
import gleam/erlang
import gleam/erlang/process
import gleam/iterator
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import glisten.{type Connection, type Message, Packet, User}
import redis/command
import redis/config.{type Config}
import redis/resp.{type Resp}
import redis/store.{type Table}
import redis/stream
import redis/value

pub fn main() {
  let config = config.load()
  let assert Ok(table) =
    config.db_full_path(config)
    |> option.map(store.load)
    |> option.unwrap(store.new())

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
            store.insert(table, key, value.String(value), None)
            resp.SimpleString("OK")
          }

          command.Set(key: key, value: value, expiry: Some(expiry)) -> {
            let deadline = erlang.system_time(erlang.Millisecond) + expiry
            store.insert(table, key, value.String(value), Some(deadline))
            resp.SimpleString("OK")
          }

          command.Get(key) -> {
            case store.lookup(table, key) {
              value.None -> resp.Null(resp.NullString)
              value.String(s) -> resp.BulkString(s)
              _ ->
                resp.SimpleError("TODO can only get strings or nothing for now")
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
            store.keys(table)
            |> iterator.map(resp.BulkString)
            |> iterator.to_list
            |> resp.Array
          }
          command.Keys(_) ->
            resp.SimpleError(
              "TODO KEYS command with matching will be implemented soon",
            )

          command.Type(key) -> {
            store.lookup(table, key)
            |> value.to_type_name
            |> resp.SimpleString
          }

          command.XAdd(stream_key, entry_id, data) ->
            {
              case store.lookup(table, stream_key) {
                value.None -> {
                  use stream <- result.map(stream.new(stream_key))
                  store.insert(
                    into: table,
                    key: stream_key,
                    value: value.Stream(stream),
                    deadline: None,
                  )
                  stream
                }
                value.Stream(stream) -> Ok(stream)
                _ -> Error("")
              }
              |> result.map(stream.handle_xadd(_, entry_id, data))
            }
            |> result.map_error(resp.SimpleError)
            |> result.unwrap_both

          command.XRange(stream, start, end) -> todo
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
