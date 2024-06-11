import bravo
import bravo/uset.{type USet}
import gleam/bytes_builder
import gleam/erlang
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import glisten.{type Connection, type Message, Packet, User}
import redis/command
import redis/config.{type Config}
import redis/resp.{type Resp}

type Value =
  #(String, Resp, Option(Int))

type State {
  State(table: USet(Value), config: Config)
}

pub fn main() {
  let assert Ok(_) =
    glisten.handler(init, router)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn router(msg: Message(a), state: State, conn: Connection(a)) {
  case msg {
    Packet(resp_binary) -> {
      let assert Ok(#(resp, _)) = resp_binary |> resp.parse

      let assert Ok(command) = command.parse(resp)
      case command {
        command.Ping -> {
          let assert Ok(_) =
            resp.SimpleString("PONG")
            |> send_resp(conn)

          actor.continue(state)
        }
        command.Echo(payload) -> {
          let assert Ok(_) = send_resp(payload, conn)
          actor.continue(state)
        }

        command.Set(key: key, value: value, expiry: None) -> {
          uset.insert(state.table, [#(key, value, None)])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(state)
        }

        command.Set(key: key, value: value, expiry: Some(expiry)) -> {
          let deadline = erlang.system_time(erlang.Millisecond) + expiry
          uset.insert(state.table, [#(key, value, Some(deadline))])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(state)
        }

        command.Get(key) -> {
          let posix = erlang.system_time(erlang.Millisecond)
          let assert Ok(_) =
            case uset.lookup(state.table, key) {
              None -> resp.Null(resp.NullString)
              Some(#(_, value, None)) -> value
              Some(#(_, value, Some(deadline))) if deadline > posix -> value
              Some(#(key, _, _)) -> {
                uset.delete_key(state.table, key)
                resp.Null(resp.NullString)
              }
            }
            |> send_resp(conn)

          actor.continue(state)
        }
        command.Config(subcommand) -> {
          let assert Ok(_) = case subcommand {
            command.ConfigGet(parameter) -> {
              case parameter {
                config.Dir -> state.config.dir
                config.DbFilename -> state.config.dbfilename
              }
              |> option.map(resp.BulkString)
              |> option.unwrap(resp.Null(resp.NullString))
              |> list.wrap
              |> list.prepend(resp.BulkString(config.parameter_key(parameter)))
              |> resp.Array
              |> send_resp(conn)
            }
          }

          actor.continue(state)
        }
        command.Keys(_) -> todo as "KEYS command will be implemented soon"
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

const store_name = "redis_on_ets"

fn init(_conn) -> #(State, Option(a)) {
  let assert Ok(table) = uset.new(store_name, 1, bravo.Public)

  #(State(table, config.load()), None)
}
