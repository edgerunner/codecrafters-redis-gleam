import gleam/bytes_builder
import gleam/erlang
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/iterator
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import glisten.{type Connection, type Message, Packet, User}
import mug
import redis/command.{type Command}
import redis/config.{type Config}
import redis/info
import redis/replication.{type Replication}
import redis/resp.{type Resp}
import redis/store.{type Table}
import redis/stream
import redis/value

type Msg =
  Subject(Int)

type State {
  State(
    waiting_for_offset: List(Subject(Int)),
    subject: Subject(Subject(Int)),
    multi: Option(List(Command)),
  )
}

pub fn main() {
  let config = config.load()
  let assert Ok(table) =
    config.db_full_path(config)
    |> option.map(store.load)
    |> option.unwrap(store.new())

  let replication = case config.replicaof {
    None -> replication.master()
    Some(#(master, port)) ->
      replication.slave(
        to: master,
        on: port,
        from: config.port,
        with: fn(msg, state, socket) {
          slave_handler(msg, state, table, socket)
        },
      )
  }

  let assert Ok(_) =
    glisten.handler(
      fn(_) {
        let subject = process.new_subject()
        let selector =
          process.new_selector()
          |> process.selecting(subject, fn(x) { x })
        #(State([], subject, None), Some(selector))
      },
      fn(msg, state, conn) {
        router(msg, state, table, config, replication, conn)
      },
    )
    |> glisten.serve(config.port)

  process.sleep_forever()
}

fn router(
  msg: Message(Msg),
  state: State,
  table: Table,
  config: Config,
  replication: Replication(Msg),
  conn: Connection(Msg),
) -> actor.Next(Message(Msg), State) {
  case msg {
    Packet(resp_binary) -> {
      let assert Ok(#(resp, _)) = resp_binary |> resp.parse
      let assert Ok(command) = command.parse(resp)
      command_handler(command, state, table, config, replication, conn)
    }
    User(subject) -> {
      State(..state, waiting_for_offset: [subject, ..state.waiting_for_offset])
      |> actor.continue
    }
  }
}

fn command_handler(
  command: Command,
  state: State,
  table: Table,
  config: Config,
  replication: Replication(Msg),
  conn: Connection(Msg),
) -> actor.Next(Message(Msg), State) {
  let send_and_continue = fn(resp) {
    let _ = send_resp(resp, conn)
    actor.continue(state)
  }
  case command {
    command.Ping -> resp.SimpleString("PONG") |> send_and_continue
    command.Echo(payload) -> send_and_continue(payload)

    command.Set(key: key, value: value, expiry: None) -> {
      store.insert(table, key, value.String(value), None)
      replication.replicate(replication, command)
      resp.SimpleString("OK") |> send_and_continue
    }

    command.Set(key: key, value: value, expiry: Some(expiry)) -> {
      let deadline = erlang.system_time(erlang.Millisecond) + expiry
      store.insert(table, key, value.String(value), Some(deadline))
      replication.replicate(replication, command)
      resp.SimpleString("OK") |> send_and_continue
    }

    command.Get(key) -> {
      case store.lookup(table, key) {
        value.None -> resp.Null(resp.NullString)
        value.String(s) -> resp.BulkString(s)
        value.Integer(i) -> resp.BulkString(int.to_string(i))
        _ ->
          resp.SimpleError(
            "TODO can only get strings, integers or nothing for now",
          )
      }
      |> send_and_continue
    }
    command.Config(subcommand) -> {
      case subcommand {
        command.ConfigGet(parameter) -> {
          config.to_string(config, parameter)
          |> option.map(resp.BulkString)
          |> option.unwrap(resp.Null(resp.NullString))
          |> list.wrap
          |> list.prepend(resp.BulkString(config.parameter_key(parameter)))
          |> resp.Array
          |> send_and_continue
        }
      }
    }
    command.Keys(None) -> {
      store.keys(table)
      |> iterator.map(resp.BulkString)
      |> iterator.to_list
      |> resp.Array
      |> send_and_continue
    }
    command.Keys(_) ->
      resp.SimpleError(
        "TODO KEYS command with matching will be implemented soon",
      )
      |> send_and_continue

    command.Type(key) -> {
      store.lookup(table, key)
      |> value.to_type_name
      |> resp.SimpleString
      |> send_and_continue
    }

    command.XAdd(stream_key, entry_id, data) ->
      {
        case store.lookup(table, stream_key) {
          value.None -> {
            use stream <- result.map(stream.new(stream_key))
            store.insert(
              into: table,
              key: stream_key,
              value: value.Stream(stream),
              deadline: None,
            )
            stream
          }
          value.Stream(stream) -> Ok(stream)
          _ -> Error("ERR " <> stream_key <> " is not a stream")
        }
        |> result.map(stream.handle_xadd(_, entry_id, data))
      }
      |> result.map_error(resp.SimpleError)
      |> result.unwrap_both
      |> do(replication.replicate(replication, command))
      |> send_and_continue

    command.XRange(stream_key, start, end) ->
      case store.lookup(table, stream_key) {
        value.None -> resp.Null(resp.NullArray)
        value.Stream(stream) -> stream.handle_xrange(stream, start, end)
        _ -> resp.SimpleError("ERR " <> stream_key <> " is not a stream")
      }
      |> send_and_continue

    command.XRead(streams, block) ->
      {
        use #(stream_key, from) <- list.map(streams)
        case store.lookup(table, stream_key) {
          value.None -> resp.Null(resp.NullArray)
          value.Stream(stream) ->
            [
              resp.BulkString(stream_key),
              option.map(block, stream.handle_xread_block(stream, from, _))
                |> option.unwrap(stream.handle_xread(stream, from)),
            ]
            |> resp.Array
          _ -> resp.SimpleError("ERR " <> stream_key <> " is not a stream")
        }
      }
      |> resp.Array
      |> send_and_continue

    command.Info(command.InfoReplication) ->
      info.handle_replication(config.replicaof, replication)
      |> send_and_continue

    command.ReplConf(command.ReplConfCapa(_)) ->
      resp.SimpleString("OK") |> send_and_continue

    command.ReplConf(command.ReplConfListeningPort(_)) ->
      resp.SimpleString("OK") |> send_and_continue

    command.ReplConf(command.ReplConfGetAck(_)) ->
      resp.SimpleError("ERR Only the master can send this")
      |> send_and_continue

    command.ReplConf(command.ReplConfAck(ack_offset)) -> {
      list.each(state.waiting_for_offset, process.send(_, ack_offset))
      actor.continue(state)
    }
    command.PSync(id, offset) -> {
      replication.handle_psync(replication, id, offset, conn, state.subject)
      |> send_and_continue
    }

    command.Wait(replicas, timeout) ->
      replication.wait(replication, replicas, timeout)
      |> result.map(resp.Integer)
      |> result.unwrap(resp.SimpleError("could not resolve replication"))
      |> send_and_continue

    command.Incr(key) -> {
      let incr = fn(int) {
        store.insert(
          into: table,
          key: key,
          value: value.Integer(int + 1),
          deadline: None,
        )
        resp.Integer(int + 1)
      }
      case store.lookup(table, key) {
        value.Integer(int) -> incr(int)
        value.None -> incr(0)
        value.String(str) ->
          int.parse(str)
          |> result.map(incr)
          |> result.replace_error(resp.SimpleError(
            "ERR value is not an integer or out of range",
          ))
          |> result.unwrap_both
        _ -> resp.SimpleError("ERR value is not an integer or out of range")
      }
      |> send_and_continue
    }
    command.Multi -> todo
  }
}

fn send_resp(resp: Resp, conn: Connection(msg)) -> Result(Nil, Nil) {
  resp.encode(resp)
  |> bytes_builder.from_bit_array
  |> glisten.send(conn, _)
  |> result.nil_error
}

fn do(prev, _x) {
  prev
}

fn slave_handler(
  resp_binary: BitArray,
  offset: Int,
  table: Table,
  socket: mug.Socket,
) -> Int {
  use offset, #(resp, command_offset) <- iterator.fold(
    over: resp.iterate(resp_binary),
    from: offset,
  )
  let assert Ok(command) = command.parse(resp)
  case command {
    command.Ping -> command_offset

    command.Set(key: key, value: value, expiry: None) -> {
      store.insert(table, key, value.String(value), None)
      command_offset
    }

    command.Set(key: key, value: value, expiry: Some(expiry)) -> {
      let deadline = erlang.system_time(erlang.Millisecond) + expiry
      store.insert(table, key, value.String(value), Some(deadline))
      command_offset
    }

    command.ReplConf(command.ReplConfGetAck(_)) -> {
      ["REPLCONF", "ACK", int.to_string(offset)]
      |> list.map(resp.BulkString)
      |> resp.Array
      |> resp.encode
      |> mug.send(socket, _)
      |> result.replace(command_offset)
      |> result.unwrap(or: 0)
    }

    _ -> 0
  }
  + offset
}
