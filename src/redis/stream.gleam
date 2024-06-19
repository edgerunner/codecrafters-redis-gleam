import bravo.{Private}
import bravo/oset.{type OSet}
import gleam/bool
import gleam/erlang
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/iterator.{type Iterator}
import gleam/list
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
  |> result.map(id_resp)
  |> result.map_error(resp.SimpleError)
  |> result.unwrap_both
}

pub fn handle_xrange(
  stream: Stream,
  start: StreamEntryId,
  end: StreamEntryId,
) -> Resp {
  range(stream, start, end)
  |> list.map(to_resp)
  |> resp.Array
}

pub fn handle_xread(stream: Stream, from: StreamEntryId) -> Resp {
  read(stream, from)
  |> list.map(to_resp)
  |> resp.Array
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

fn last_id(stream: Stream) -> Id {
  case last(stream) {
    Ok(Entry(id, _)) -> id
    Error(_) -> #(0, 0)
  }
}

fn to_resp(entry: Entry) -> Resp {
  let Entry(_, data) = entry
  let id_resp = id_string(entry) |> resp.BulkString
  let data_resp =
    list.flat_map(data, fn(kv) { [kv.0, kv.1] })
    |> list.map(resp.BulkString)
    |> resp.Array
  resp.Array([id_resp, data_resp])
}

fn id_string(entry: Entry) -> String {
  let Entry(#(timestamp, sequence), _) = entry
  int.to_string(timestamp) <> "-" <> int.to_string(sequence)
}

fn id_resp(id: Id) -> Resp {
  let #(timestamp, sequence) = id
  resp.BulkString(int.to_string(timestamp) <> "-" <> int.to_string(sequence))
}

// Stream actor

pub opaque type Message {
  Add(entry: Entry, expect: Subject(Result(Id, String)))
  Last(expect: Subject(Result(Entry, Nil)))
  Range(start: StreamEntryId, end: StreamEntryId, expect: Subject(List(Entry)))
  Read(from: StreamEntryId, expect: Subject(List(Entry)))
}

fn add(entry: Entry, stream: Stream) -> Result(Id, String) {
  use sender <- actor.call(stream, _, 500)
  Add(entry, sender)
}

fn last(stream: Stream) -> Result(Entry, Nil) {
  use sender <- actor.call(stream, _, 500)
  Last(sender)
}

fn range(
  stream: Stream,
  start: StreamEntryId,
  end: StreamEntryId,
) -> List(Entry) {
  use sender <- actor.call(stream, _, 500)
  Range(start, end, sender)
}

fn read(stream: Stream, from: StreamEntryId) -> List(Entry) {
  use sender <- actor.call(stream, _, 500)
  Read(from, sender)
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

    Range(start, end, sender) -> {
      keys(stream_data)
      |> iterator.filter_map(oset.lookup(stream_data, _))
      |> iterator.map(from_stored)
      |> iterator.drop_while(smaller_than_id(_, start))
      |> iterator.take_while(not_larger_than_id(_, end))
      |> iterator.to_list
      |> actor.send(sender, _)
    }

    Read(from, sender) -> {
      keys(stream_data)
      |> iterator.filter_map(oset.lookup(stream_data, _))
      |> iterator.map(from_stored)
      |> iterator.drop_while(not_larger_than_id(_, from))
      |> iterator.to_list
      |> actor.send(sender, _)
    }
  }

  actor.continue(stream_data)
}

fn to_stored(entry: Entry) -> #(String, Entry) {
  #(id_string(entry), entry)
}

fn from_stored(stored: #(String, Entry)) -> Entry {
  stored.1
}

fn keys(table: StreamDataset) -> Iterator(String) {
  use key <- iterator.unfold(from: oset.first(table))

  case key {
    Error(Nil) -> iterator.Done
    Ok(key) -> iterator.Next(key, oset.next(table, key))
  }
}

fn smaller_than_id(entry: Entry, id: StreamEntryId) -> Bool {
  let Entry(#(timestamp, sequence), _) = entry
  case id {
    command.Unspecified -> False
    command.Timestamp(id_timestamp) -> timestamp < id_timestamp
    command.Explicit(id_timestamp, id_sequence) ->
      timestamp < id_timestamp
      || { timestamp == id_timestamp && sequence < id_sequence }
  }
}

fn not_larger_than_id(entry: Entry, id: StreamEntryId) -> Bool {
  let Entry(#(timestamp, sequence), _) = entry
  case id {
    command.Unspecified -> True
    command.Timestamp(id_timestamp) -> timestamp <= id_timestamp
    command.Explicit(id_timestamp, id_sequence) ->
      timestamp < id_timestamp
      || { timestamp == id_timestamp && sequence <= id_sequence }
  }
}
