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

import gleam/list
import mug
import redis/resp

pub fn slave(to host: String, on port: Int, from listening_port: Int) {
  io.println("Connecting to master: " <> host)
  let options = mug.new(host, port)
  let assert Ok(socket) = mug.connect(options)

  io.print("PING …")
  let assert "PONG" = send_command(socket, ["PING"])
  io.println(" PONG")

  let replconf1 = ["REPLCONF", "listening-port", int.to_string(listening_port)]
  io.print(string.join(replconf1, " ") <> " …")
  let assert "OK" = send_command(socket, replconf1)
  io.println(" OK")

  let replconf2 = ["REPLCONF", "capa", "psync2"]
  io.print(string.join(replconf2, " ") <> " …")
  let assert "OK" = send_command(socket, replconf2)
  io.println(" OK")

  let psync = ["PSYNC", "?", "-1"]
  io.print("PSYNC …")
  let psync = send_command(socket, psync)
  io.println(psync)

  Slave(master_replid: "", master_repl_offset: -1)
}

fn send_command(socket: mug.Socket, parts: List(String)) {
  let assert Ok(_) =
    list.map(parts, resp.BulkString)
    |> resp.Array
    |> resp.encode
    |> mug.send(socket, _)
  let assert Ok(response) = mug.receive(socket, 10_000)
  let assert Ok(#(resp.SimpleString(payload), _)) = resp.parse(response)
  payload
}

import gleam/io
