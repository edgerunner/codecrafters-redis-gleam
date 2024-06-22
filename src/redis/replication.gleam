pub type Replication {
  Master(master_replid: String, master_repl_offset: Int)
  Slave(master_replid: String, master_repl_offset: Int)
}

pub fn master() -> Replication {
  Master(master_replid: random_replid(), master_repl_offset: 0)
}

import gleam/int
import gleam/iterator
import gleam/string

fn random_replid() -> String {
  fn() {
    case int.random(36) {
      num if num < 10 -> num + 0x30
      alpha -> alpha + 0x57
    }
  }
  |> iterator.repeatedly
  |> iterator.filter_map(string.utf_codepoint)
  |> iterator.take(40)
  |> iterator.to_list
  |> string.from_utf_codepoints
}

import mug
import redis/resp

pub fn slave(to host: String, on port: Int) {
  io.println("Connecting to master: " <> host)
  let options = mug.new(host, port)
  let assert Ok(socket) = mug.connect(options)

  io.print("PING …")
  let assert Ok(_) = ping() |> resp.encode |> mug.send(socket, _)
  let assert Ok(response) = mug.receive(socket, 10_000)
  let assert Ok(#(resp.SimpleString("PONG"), _)) = resp.parse(response)
  io.println(" PONG")

  Slave(master_replid: "", master_repl_offset: -1)
}

fn ping() {
  [resp.BulkString("PING")] |> resp.Array
}

import gleam/io
