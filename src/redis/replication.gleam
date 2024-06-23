import bravo.{Public}
import bravo/bag.{type Bag}

pub type Replication {
  Master(
    master_replid: String,
    master_repl_offset: Int,
    slaves: Bag(#(String, glisten.Connection(Nil))),
  )
  Slave(master_replid: String, master_repl_offset: Int, socket: mug.Socket)
}

pub fn master() -> Replication {
  let assert Ok(slaves) = bag.new("slaves", 1, Public)
  Master(master_replid: random_replid(), master_repl_offset: 0, slaves: slaves)
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

import gleam/io
import gleam/list
import mug
import redis/resp

pub fn slave(to host: String, on port: Int, from listening_port: Int) {
  io.println("Connecting to master: " <> host)
  let options = mug.new(host, port)
  let assert Ok(socket) = mug.connect(options)

  io.print("PING … ")
  let assert "PONG" = send_command(socket, ["PING"])
  io.println("PONG")

  let replconf1 = ["REPLCONF", "listening-port", int.to_string(listening_port)]
  io.print(string.join(replconf1, " ") <> " … ")
  let assert "OK" = send_command(socket, replconf1)
  io.println("OK")

  let replconf2 = ["REPLCONF", "capa", "psync2"]
  io.print(string.join(replconf2, " ") <> " … ")
  let assert "OK" = send_command(socket, replconf2)
  io.println("OK")

  let psync = ["PSYNC", "?", "-1"]
  io.print("PSYNC … ")
  let psync = send_command(socket, psync)
  io.println(psync)

  let assert ["FULLRESYNC", replid, offset] = string.split(psync, " ")
  let assert Ok(offset) = int.parse(offset)

  io.print("Waiting for RDB file … ")
  let assert Ok(rdb) = mug.receive(socket, 10_000)
  io.print("parsing … ")
  let assert Ok(#(resp.BulkData(rdb), _)) = resp.parse(rdb)
  io.print("checking … ")
  let assert True = rdb == rdb.empty
  io.println("OK")

  io.println("Connected to master: " <> host)
  Slave(master_replid: replid, master_repl_offset: offset, socket: socket)
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

import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{type Option, None}
import gleam/otp/actor
import glisten
import redis/rdb

pub fn handle_psync(
  replication: Replication,
  id: Option(String),
  offset: Int,
  conn: glisten.Connection(Nil),
) -> resp.Resp {
  case replication, id, offset {
    Master(master_repl_id, master_repl_offset, slaves), None, -1 -> {
      let assert Ok(send_rdb) =
        actor.start(Nil, fn(_, _) {
          let assert Ok(_) =
            rdb.empty
            |> resp.BulkData
            |> resp.encode
            |> bytes_builder.from_bit_array
            |> glisten.send(conn, _)

          actor.Stop(process.Normal)
        })

      bag.insert(slaves, [#(master_repl_id, conn)])

      process.send_after(send_rdb, 50, Nil)

      ["FULLRESYNC", master_repl_id, int.to_string(master_repl_offset)]
      |> string.join(" ")
      |> resp.SimpleString
    }
    Master(_, _, _), _, _ -> todo
    Slave(_, _, _), _, _ -> resp.SimpleError("ERR This instance is a slave")
  }
}
