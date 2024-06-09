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
            |> resp.encode
            |> bytes_builder.from_bit_array
            |> glisten.send(conn, _)

          actor.continue(state)
        }
        "ECHO", [payload, ..] -> {
          let assert Ok(_) =
            payload
            |> resp.encode
            |> bytes_builder.from_bit_array
            |> glisten.send(conn, _)

          actor.continue(state)
        }

        "SET", [key, value, ..] -> {
          let assert Ok(key) = resp.to_string(key)
          table.insert(state, [#(key, value)])
          let assert Ok(_) =
            resp.SimpleString("OK")
            |> resp.encode
            |> bytes_builder.from_bit_array
            |> glisten.send(conn, _)

          actor.continue(state)
        }

        "GET", [key, ..] -> {
          let assert Ok(key) = resp.to_string(key)
          let assert Ok(_) =
            case table.lookup(state, key) {
              [] -> resp.Null(resp.NullPrimitive)
              [#(_, value), ..] -> value
            }
            |> resp.encode
            |> bytes_builder.from_bit_array
            |> glisten.send(conn, _)

          actor.continue(state)
        }

        _, _ -> todo
      }
    }
    User(_) -> todo
  }
}

const store_name = "redis"

fn init(_conn) -> #(Set(String, Resp), Option(a)) {
  let assert Ok(table) = {
    use <- result.lazy_or(table.ref(store_name))

    table.build(store_name)
    |> table.privacy(table.Public)
    |> table.write_concurrency(table.AutoWriteConcurrency)
    |> table.read_concurrency(True)
    |> table.decentralized_counters(True)
    |> table.compression(False)
    |> table.set
  }

  #(table, None)
}
