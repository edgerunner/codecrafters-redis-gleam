import gleam/bit_array
import gleam/bytes_builder
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None}
import gleam/otp/actor
import gleam/result
import gleam/string
import glisten.{type Connection, type Message, Packet, User}
import redis/resp

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, router)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn router(msg: Message(a), state: Nil, conn: Connection(a)) {
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

        _, _ -> todo
      }
    }
    User(_) -> todo
  }
}
