import bravo.{Public}
import bravo/bag.{type Bag}
import gleam/bytes_builder
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/iterator
import gleam/list
import gleam/option.{type Option, None}
import gleam/otp/actor
import gleam/otp/task
import gleam/result
import gleam/string
import glisten
import mug
import redis/rdb
import redis/resp.{type Resp}

pub type Replication {
  Master(
    master_replid: String,
    master_repl_offset: Int,
    slaves: Bag(#(String, glisten.Connection(Nil))),
  )
  Slave(master_replid: String)
}

pub fn master() -> Replication {
  let assert Ok(slaves) = bag.new("slaves", 1, Public)
  Master(master_replid: random_replid(), master_repl_offset: 0, slaves: slaves)
}

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

pub fn slave(
  to host: String,
  on port: Int,
  from listening_port: Int,
  with handler: fn(BitArray, Int) -> Int,
) {
  let when_ready: Subject(Replication) = process.new_subject()
  let slave_spec: actor.Spec(Int, mug.TcpMessage) =
    actor.Spec(
      init_timeout: 10_000,
      loop: fn(msg, state) {
        case msg {
          mug.Packet(_, bits) -> {
            handler(bits, state) |> actor.continue
          }
          mug.SocketClosed(_) -> actor.Stop(process.Normal)
          mug.TcpError(_, _) ->
            actor.Stop(process.Abnormal(reason: "TCP error"))
        }
      },
      init: fn() {
        let #(replid, socket) =
          slave_init(to: host, on: port, from: listening_port)

        let selector =
          process.new_selector()
          |> mug.selecting_tcp_messages(fn(msg) {
            mug.receive_next_packet_as_message(socket)
            msg
          })

        mug.receive_next_packet_as_message(socket)
        actor.send(when_ready, Slave(replid))

        actor.Ready(0, selector)
      },
    )

  io.println("Starting slave actor …")
  let assert Ok(_slave_process) = actor.start_spec(slave_spec)
  io.println("Started slave actor")

  process.receive(when_ready, 10_000)
  |> result.lazy_unwrap(or: fn() {
    panic as "Cannot start slave listener process"
  })
}

fn slave_init(to host: String, on port: Int, from listening_port: Int) {
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
  let assert Ok(_offset) = int.parse(offset)

  let _ =
    {
      io.print("Waiting for RDB file … ")
      use rdb <- result.then(mug.receive(socket, 10_000) |> result.nil_error)
      io.print("parsing … ")
      use #(rdb, _) <- result.then(resp.parse(rdb) |> result.nil_error)
      let assert resp.BulkData(rdb) = rdb
      io.print("checking … ")
      use rdb <- result.then(rdb.parse(rdb) |> result.nil_error)
      io.println("OK")
      Ok(rdb)
    }
    |> result.map_error(fn(_) { io.println("FAIL") })

  io.println("Connected to master: " <> host)
  #(replid, socket)
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

pub fn handle_psync(
  replication: Replication,
  id: Option(String),
  offset: Int,
  conn: glisten.Connection(Nil),
) -> resp.Resp {
  case replication, id, offset {
    Master(master_repl_id, master_repl_offset, slaves), None, -1 -> {
      task.async(fn() {
        process.sleep(50)
        let assert Ok(_) =
          rdb.empty
          |> resp.BulkData
          |> resp.encode
          |> bytes_builder.from_bit_array
          |> glisten.send(conn, _)
      })

      bag.insert(slaves, [#(master_repl_id, conn)])

      ["FULLRESYNC", master_repl_id, int.to_string(master_repl_offset)]
      |> string.join(" ")
      |> resp.SimpleString
    }
    Master(_, _, _), _, _ -> todo
    Slave(_), _, _ -> resp.SimpleError("ERR This instance is a slave")
  }
}

pub fn replicate(replication: Replication, command: Resp) {
  case replication {
    Master(id, _, slaves) -> {
      use #(_, conn) <- list.filter_map(bag.lookup(slaves, id))
      resp.encode(command)
      |> bytes_builder.from_bit_array
      |> glisten.send(conn, _)
    }

    Slave(_) -> todo
  }
}
