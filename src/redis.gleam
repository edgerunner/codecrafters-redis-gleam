import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor
import glisten

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, fn(_msg, state, conn) {
      let pong = bytes_builder.from_string("+PONG\r\n")
      let assert Ok(_) = glisten.send(conn, pong)
      actor.continue(state)
    })
    |> glisten.serve(6379)

  process.sleep_forever()
}
