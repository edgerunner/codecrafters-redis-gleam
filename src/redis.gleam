import carpenter/table.{type Set}
import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{type Option, None}
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{type Connection, type Message, Packet, User}
import redis/resp.{type Resp}

type State =
  Set(String, Resp)

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
      let assert resp.Array([command, ..args]) = resp
      let assert Ok(command) = resp.to_string(command)
      case string.uppercase(command), args {
        "PING", _ -> {
          let assert Ok(_) =
            resp.SimpleString("PONG")
            |> send_resp(conn)

          actor.continue(state)
        }
        "ECHO", [payload, ..] -> {
          let assert Ok(_) = send_resp(payload, conn)
          actor.continue(state)
        }

        "SET", [key, value, ..] -> {
          let assert Ok(key) = resp.to_string(key)
          table.insert(state, [#(key, value)])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> send_resp(conn)
          actor.continue(state)
        }

        "GET", [key, ..] -> {
          let assert Ok(key) = resp.to_string(key)
          let assert Ok(_) =
            case table.lookup(state, key) {
              [] -> resp.Null(resp.NullPrimitive)
              [#(_, value), ..] -> value
            }
            |> send_resp(conn)

          actor.continue(state)
        }

        _, _ -> todo
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

fn init(_conn) -> #(Set(String, Resp), Option(a)) {
  let assert Ok(table) = {
    use <- result.lazy_or(table.ref(store_name))

    table.build(store_name)
    |> table.privacy(table.Public)
    |> table.write_concurrency(table.WriteConcurrency)
    |> table.read_concurrency(True)
    |> table.decentralized_counters(True)
    |> table.compression(False)
    |> table.set
  }

  #(table, None)
}
