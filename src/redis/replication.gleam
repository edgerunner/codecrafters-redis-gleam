import bravo.{Public}
import bravo/bag.{type Bag}
import counter
import gleam/bool
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

pub type Replication(msg) {
  Master(
    master_replid: String,
    offset: Subject(Offset),
    slaves: Bag(#(String, glisten.Connection(msg), Subject(msg))),
  )
  Slave(master_replid: String)
}

pub opaque type Offset {
  IncrementOffset(Int)
  GetOffset(Subject(Int))
}

pub fn master() -> Replication(a) {
  let assert Ok(slaves) = bag.new("slaves", 1, Public)
  let assert Ok(offset_subject) =
    actor.start(0, fn(msg, current) {
      case msg {
        GetOffset(sender) -> {
          actor.send(sender, current)
          actor.continue(current)
        }
        IncrementOffset(amount) -> actor.continue(current + amount)
      }
    })
  Master(master_replid: random_replid(), offset: offset_subject, slaves: slaves)
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
  with handler: fn(BitArray, Int, mug.Socket) -> Int,
) {
  let when_ready: Subject(Replication(a)) = process.new_subject()
  let slave_spec: actor.Spec(Int, mug.TcpMessage) =
    actor.Spec(
      init_timeout: 10_000,
      loop: fn(msg, state) {
        case msg {
          mug.Packet(socket, bits) -> {
            handler(bits, state, socket) |> actor.continue
          }
          mug.SocketClosed(_) -> actor.Stop(process.Normal)
          mug.TcpError(_, _) ->
            actor.Stop(process.Abnormal(reason: "TCP error"))
        }
      },
      init: fn() {
        let #(replid, socket, rest) =
          slave_init(to: host, on: port, from: listening_port)

        let selector =
          process.new_selector()
          |> mug.selecting_tcp_messages(fn(msg) {
            mug.receive_next_packet_as_message(socket)
            msg
          })

        mug.receive_next_packet_as_message(socket)
        actor.send(when_ready, Slave(replid))

        let offset = handler(rest, 0, socket)

        actor.Ready(offset, selector)
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
  let assert #("PONG", <<>>) = send_command(socket, ["PING"])
  io.println("PONG")

  let replconf1 = ["REPLCONF", "listening-port", int.to_string(listening_port)]
  io.print(string.join(replconf1, " ") <> " … ")
  let assert #("OK", <<>>) = send_command(socket, replconf1)
  io.println("OK")

  let replconf2 = ["REPLCONF", "capa", "psync2"]
  io.print(string.join(replconf2, " ") <> " … ")
  let assert #("OK", <<>>) = send_command(socket, replconf2)
  io.println("OK")

  let psync = ["PSYNC", "?", "-1"]
  io.print("PSYNC … ")
  let #(psync, rest) = send_command(socket, psync)
  io.println(psync)

  let assert ["FULLRESYNC", replid, offset] = string.split(psync, " ")
  let assert Ok(_offset) = int.parse(offset)

  let rest =
    {
      io.print("Waiting for RDB file … ")
      let possible_rdb = case rest {
        <<>> -> {
          mug.receive(socket, 1000) |> result.replace_error(<<>>)
        }
        data -> Ok(data)
      }
      use bits <- result.then(possible_rdb)
      io.print("parsing … ")
      use #(resp, rest) <- result.then(
        resp.parse(bits) |> result.replace_error(bits),
      )
      let assert resp.BulkData(data) = resp
      io.print("checking … ")
      use _rdb <- result.then(rdb.parse(data) |> result.replace_error(rest))
      io.println("OK")
      Ok(rest)
    }
    |> result.map_error(fn(rest) {
      io.println("FAIL")
      rest
    })
    |> result.unwrap_both

  io.println("Connected to master: " <> host)
  #(replid, socket, rest)
}

fn send_command(socket: mug.Socket, parts: List(String)) {
  let assert Ok(_) =
    list.map(parts, resp.BulkString)
    |> resp.Array
    |> resp.encode
    |> mug.send(socket, _)
  let assert Ok(response) = mug.receive(socket, 10_000)
  let assert Ok(#(resp.SimpleString(payload), rest)) = resp.parse(response)
  #(payload, rest)
}

pub fn handle_psync(
  replication: Replication(msg),
  id: Option(String),
  offset: Int,
  conn: glisten.Connection(msg),
  subject: Subject(msg),
) -> Resp {
  case replication, id, offset {
    Master(master_repl_id, offset, slaves), None, -1 -> {
      task.async(fn() {
        process.sleep(50)
        let assert Ok(_) =
          rdb.empty
          |> resp.BulkData
          |> resp.encode
          |> bytes_builder.from_bit_array
          |> glisten.send(conn, _)
      })

      bag.insert(slaves, [#(master_repl_id, conn, subject)])

      let master_repl_offset = actor.call(offset, GetOffset, 100)

      ["FULLRESYNC", master_repl_id, int.to_string(master_repl_offset)]
      |> string.join(" ")
      |> resp.SimpleString
    }
    Master(_, _, _), _, _ -> todo
    Slave(_), _, _ -> todo
  }
}

pub fn replicate(replication: Replication(a), command: Resp) {
  case replication {
    Master(id, offset, slaves) -> {
      let resp = resp.encode(command) |> bytes_builder.from_bit_array
      actor.send(offset, IncrementOffset(bytes_builder.byte_size(resp)))
      use #(_, conn, _) <- list.filter_map(bag.lookup(slaves, id))
      glisten.send(conn, resp)
    }

    Slave(_) -> todo
  }
}

pub fn slave_count(replication: Replication(a)) -> Int {
  case replication {
    Master(id, _offset, slaves) -> bag.lookup(slaves, id) |> list.length
    Slave(_) -> 0
  }
}

pub fn wait(
  replication: Replication(Subject(Int)),
  replicas replicas_required: Int,
  timeout timeout: Int,
) {
  case replication {
    Master(id, offset, slaves) -> {
      let master_offset = actor.call(offset, GetOffset, 10)
      use <- bool.guard(
        when: master_offset == 0,
        return: slave_count(replication) |> Ok,
      )
      let #(count, done) =
        counter.start(
          until: replicas_required,
          for: timeout,
          counting: fn(slave_offset) { slave_offset >= master_offset },
        )

      {
        use #(_id, _conn, subject) <- list.each(bag.lookup(slaves, id))
        actor.send(subject, count)
      }
      ["REPLCONF", "GETACK", "*"]
      |> list.map(resp.BulkString)
      |> resp.Array
      |> replicate(replication, _)

      process.receive(done, within: timeout + 1000)
    }
    Slave(_id) -> todo as "forward to master"
  }
}
