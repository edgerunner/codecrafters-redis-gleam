import bravo.{Private}
import bravo/oset.{type OSet}
import gleam/bool
import gleam/erlang
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/order.{Eq, Gt, Lt}
import gleam/otp/actor
import gleam/result
import redis/command.{type StreamEntryId}
import redis/resp.{type Resp}

pub type Stream =
  Subject(Message)

type Entry {
  Entry(id: Id, data: Data)
}

type Id =
  #(Int, Int)

type Data =
  List(#(String, String))

pub fn handle_xadd(
  stream: Stream,
  entry_id: StreamEntryId,
  data: List(#(String, String)),
) -> Resp {
  case entry_id {
    command.Unspecified -> {
      let time = erlang.system_time(erlang.Millisecond)
      let #(last_ts, last_seq) = last_id(stream)
      let id = case int.compare(last_ts, time) {
        Lt -> #(time, 0)
        Eq | Gt -> #(last_ts, last_seq + 1)
      }
      add(Entry(id, data), stream)
    }
    command.Timestamp(timestamp) -> {
      let #(last_ts, last_seq) = last_id(stream)
      let sequence = case int.compare(last_ts, timestamp) {
        Lt | Gt -> 0
        Eq -> last_seq + 1
      }
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: last_ts,
        last_seq: last_seq,
      )
      add(Entry(#(timestamp, sequence), data), stream)
    }
    command.Explicit(timestamp, sequence) -> {
      let #(last_ts, last_seq) = last_id(stream)
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: last_ts,
        last_seq: last_seq,
      )
      add(Entry(#(timestamp, sequence), data), stream)
    }
  }
  |> result.map(fn(entries) {
    let #(timestamp, sequence) = entries
    resp.stream_entry_id(timestamp, sequence)
  })
  |> result.map_error(resp.SimpleError)
  |> result.unwrap_both
}

fn validate_entry_id(
  last_ts last_ts: Int,
  last_seq last_seq: Int,
  timestamp timestamp: Int,
  sequence sequence: Int,
  when_valid callback: fn() -> Result(a, String),
) {
  use <- bool.guard(
    when: timestamp < 0 || { timestamp == 0 && sequence < 1 },
    return: Error("ERR The ID specified in XADD must be greater than 0-0"),
  )
  bool.guard(
    when: timestamp < last_ts
      || { timestamp == last_ts && sequence <= last_seq },
    return: Error(
      "ERR The ID specified in XADD is equal or smaller than the target stream top item",
    ),
    otherwise: callback,
  )
}

fn last_id(stream: Stream) -> #(Int, Int) {
  case last(stream) {
    Ok(Entry(id, _)) -> id
    Error(_) -> #(0, 0)
  }
}

// Stream actor

pub opaque type Message {
  Add(entry: Entry, expect: Subject(Result(Id, String)))
  Last(expect: Subject(Result(Entry, Nil)))
}

fn add(entry: Entry, stream: Stream) -> Result(Id, String) {
  use sender <- actor.call(stream, _, 500)
  Add(entry, sender)
}

fn last(stream: Stream) -> Result(Entry, Nil) {
  use sender <- actor.call(stream, _, 500)
  Last(sender)
}

pub fn new(key: String) -> Result(Stream, String) {
  let init = fn() {
    case oset.new(name: "stream:" <> key, keypos: 1, access: Private) {
      Ok(stream) -> {
        actor.Ready(state: stream, selector: process.new_selector())
      }
      Error(bravo_error) -> actor.Failed(erlang.format(bravo_error))
    }
  }
  use error <- result.map_error(actor.start_spec(actor.Spec(init, 1000, loop)))
  case error {
    actor.InitTimeout -> "The stream timed out"
    actor.InitFailed(process.Abnormal(reason)) ->
      key <> " - Stream initialization failed: " <> reason
    actor.InitFailed(term) ->
      key <> " - Stream initialization failed: " <> erlang.format(term)
    actor.InitCrashed(term) ->
      key <> " - Stream crashed during init with: " <> erlang.format(term)
  }
}

type StreamDataset =
  OSet(#(String, Entry))

fn loop(
  message: Message,
  stream_data: StreamDataset,
) -> actor.Next(Message, StreamDataset) {
  case message {
    Add(Entry(id, _) as entry, sender) -> {
      case oset.insert_new(stream_data, [to_stored(entry)]) {
        True -> Ok(id)
        False -> Error("Could not insert entry into stream")
      }
      |> actor.send(sender, _)
    }

    Last(sender) -> {
      oset.last(stream_data)
      |> result.then(oset.lookup(stream_data, _))
      |> result.map(from_stored)
      |> actor.send(sender, _)
    }
  }

  actor.continue(stream_data)
}

fn to_stored(entry: Entry) -> #(String, Entry) {
  let Entry(#(timestamp, sequence), _) = entry
  #(int.to_string(timestamp) <> "-" <> int.to_string(sequence), entry)
}

fn from_stored(stored: #(String, Entry)) {
  stored.1
}
