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

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, router)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn router(msg: Message(a), state: Nil, conn: Connection(a)) {
  case msg {
    Packet(resp_binary) -> {
      let resp = resp_binary |> bit_array.to_string |> result.then(decode_resp)
      case resp {
        Ok(SimpleString("PING")) -> {
          let assert Ok(_) =
            glisten.send(
              conn,
              encode_resp(SimpleString("PONG")) |> bytes_builder.from_string,
            )
          actor.continue(state)
        }
        Ok(BulkString(_)) -> {
          let assert Ok(_) =
            glisten.send(
              conn,
              encode_resp(SimpleString("PONG")) |> bytes_builder.from_string,
            )
          actor.continue(state)
        }
        _ -> todo
      }
    }
    User(_) -> todo
  }
}

type Resp {
  SimpleString(String)
  BulkString(String)
  Array(List(Resp))
}

const rn = "\r\n"

fn decode_resp(resp: String) -> Result(Resp, Nil) {
  case resp {
    "+" <> data -> data |> string.drop_right(2) |> SimpleString |> Ok
    "$" <> data -> {
      let assert [_length, string] = string.split(data, on: rn)
      Ok(BulkString(string))
    }
    _ -> Error(Nil)
  }
}

fn encode_resp(resp: Resp) -> String {
  case resp {
    SimpleString(str) -> "+" <> str <> rn
    BulkString(str) -> {
      let len = string.length(str) |> int.to_string
      "$" <> len <> rn <> str <> rn
    }
    Array(items) -> {
      let header = "*" <> { list.length(items) |> int.to_string } <> rn
      use output, current <- list.fold(over: items, from: header)
      output <> encode_resp(current) <> rn
    }
  }
}
