import gleam/bool
import gleam/erlang
import gleam/int
import gleam/option.{None}
import gleam/order.{Eq, Gt, Lt}
import gleam/result
import redis/command.{type StreamEntryId}
import redis/resp.{type Resp}
import redis/store.{type Table}
import redis/value

pub fn handle_xadd(
  table: Table,
  stream: String,
  entry_id: StreamEntryId,
  data: List(#(String, String)),
) -> Resp {
  case store.lookup(table, stream), entry_id {
    // New stream, auto id
    value.None, command.AutoGenerate -> {
      let timestamp = erlang.system_time(erlang.Millisecond)
      Ok([#(timestamp, 0, data)])
    }
    // New stream, auto sequence
    value.None, command.AutoSequence(timestamp) -> {
      let sequence = case timestamp {
        0 -> 1
        _ -> 0
      }
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: 0,
        last_seq: 0,
      )
      Ok([#(timestamp, sequence, data)])
    }
    // New stream, explicit id
    value.None, command.Explicit(timestamp, sequence) -> {
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: 0,
        last_seq: 0,
      )
      Ok([#(timestamp, sequence, data)])
    }
    // Existing stream, auto id
    value.Stream([#(last_ts, last_seq, _), ..] as entries), command.AutoGenerate
    -> {
      let time = erlang.system_time(erlang.Millisecond)
      let #(timestamp, sequence) = case int.compare(last_ts, time) {
        Lt -> #(time, 0)
        Eq | Gt -> #(last_ts, last_seq + 1)
      }
      Ok([#(timestamp, sequence, data), ..entries])
    }
    // Existing stream, auto sequence
    value.Stream([#(last_ts, last_seq, _), ..] as entries),
      command.AutoSequence(timestamp)
    -> {
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
      Ok([#(timestamp, sequence, data), ..entries])
    }
    // Existing stream, explicit id
    value.Stream([#(last_ts, last_seq, _), ..] as entries),
      command.Explicit(timestamp, sequence)
    -> {
      use <- validate_entry_id(
        timestamp: timestamp,
        sequence: sequence,
        last_ts: last_ts,
        last_seq: last_seq,
      )
      Ok([#(timestamp, sequence, data), ..entries])
    }
    _, _ -> Error(resp.Null(resp.NullString))
  }
  |> result.map(fn(entries) {
    let assert [#(timestamp, sequence, _), ..] = entries
    store.insert(table, stream, value.Stream(entries), None)
    resp.stream_entry_id(timestamp, sequence)
  })
  |> result.unwrap_both
}

fn validate_entry_id(
  last_ts last_ts: Int,
  last_seq last_seq: Int,
  timestamp timestamp: Int,
  sequence sequence: Int,
  when_valid callback: fn() -> Result(a, Resp),
) {
  use <- bool.guard(
    when: timestamp < 0 || { timestamp == 0 && sequence < 1 },
    return: Error(resp.SimpleError(
      "ERR The ID specified in XADD must be greater than 0-0",
    )),
  )
  bool.guard(
    when: timestamp < last_ts
      || { timestamp == last_ts && sequence <= last_seq },
    return: Error(resp.SimpleError(
      "ERR The ID specified in XADD is equal or smaller than the target stream top item",
    )),
    otherwise: callback,
  )
}
