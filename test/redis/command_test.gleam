import gleam/option
import gleeunit/should
import redis/command
import redis/config
import redis/resp

pub fn parse_ping_test() {
  resp.SimpleString("PING")
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Ping)
}

pub fn parse_ping_mixed_case_test() {
  resp.SimpleString("PinG")
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Ping)
}

pub fn fail_parse_echo_no_argument_test() {
  [resp.BulkString("ECHO")]
  |> resp.Array
  |> command.parse
  |> should.be_error
  |> should.equal(command.WrongNumberOfArguments)
}

pub fn parse_set_key_value_test() {
  [resp.BulkString("set"), resp.BulkString("key"), resp.BulkString("value")]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Set(key: "key", value: "value", expiry: option.None))
}

pub fn parse_set_key_value_with_expiry_test() {
  [
    resp.BulkString("set"),
    resp.BulkString("key"),
    resp.BulkString("value"),
    resp.BulkString("ex"),
    resp.BulkString("15"),
  ]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Set(
    key: "key",
    value: "value",
    expiry: option.Some(15_000),
  ))
}

pub fn parse_set_key_value_with_precise_expiry_test() {
  [
    resp.BulkString("set"),
    resp.BulkString("key"),
    resp.BulkString("value"),
    resp.BulkString("Px"),
    resp.BulkString("125"),
  ]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Set(
    key: "key",
    value: "value",
    expiry: option.Some(125),
  ))
}

pub fn parse_get_key_test() {
  [resp.BulkString("Get"), resp.BulkString("key")]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Get("key"))
}

pub fn fail_parse_get_too_many_arguments_test() {
  [
    resp.BulkString("GET"),
    resp.BulkString("key"),
    resp.BulkString("value"),
    resp.BulkString("ex"),
    resp.BulkString("15"),
  ]
  |> resp.Array
  |> command.parse
  |> should.be_error
  |> should.equal(command.WrongNumberOfArguments)
}

pub fn parse_config_get_dir_test() {
  [resp.BulkString("CONFIG"), resp.BulkString("Get"), resp.BulkString("dir")]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Config(command.ConfigGet(config.Dir)))
}

pub fn parse_config_get_dbfilename_test() {
  [
    resp.BulkString("CONFIG"),
    resp.BulkString("GET"),
    resp.BulkString("dbfilename"),
  ]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Config(command.ConfigGet(config.DbFilename)))
}

pub fn parse_keys_star_test() {
  [resp.BulkString("KEYS"), resp.BulkString("*")]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Keys(option.None))
}

pub fn parse_type_test() {
  [resp.BulkString("TYPE"), resp.BulkString("flagon")]
  |> resp.Array
  |> command.parse
  |> should.be_ok
  |> should.equal(command.Type("flagon"))
}

import gleam/list
import gleam/string

pub fn parse_xadd_explicit_test() {
  "XADD fruits 12345678-0 mango 5 kiwi 48 apple 12"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(
    command.XAdd(stream: "fruits", id: command.Explicit(12_345_678, 0), data: [
      #("mango", "5"),
      #("kiwi", "48"),
      #("apple", "12"),
    ]),
  )
}

pub fn parse_xadd_auto_sequence_test() {
  "XADD fruits 12345678-* mango 15 apple 12"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(
    command.XAdd(stream: "fruits", id: command.Timestamp(12_345_678), data: [
      #("mango", "15"),
      #("apple", "12"),
    ]),
  )
}

pub fn parse_xadd_auto_test() {
  "XADD fruits * banana none mango 15 apple 12"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(
    command.XAdd(stream: "fruits", id: command.Unspecified, data: [
      #("banana", "none"),
      #("mango", "15"),
      #("apple", "12"),
    ]),
  )
}

pub fn parse_xrange_explicit_test() {
  "XRANGE fruits 1526985054069-0 1526985054079-5"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRange(
    stream: "fruits",
    start: command.Explicit(1_526_985_054_069, 0),
    end: command.Explicit(1_526_985_054_079, 5),
  ))
}

pub fn parse_xrange_timestamp_test() {
  "XRANGE fruits 1526985054069 1526985054079"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRange(
    stream: "fruits",
    start: command.Timestamp(1_526_985_054_069),
    end: command.Timestamp(1_526_985_054_079),
  ))
}

pub fn parse_xrange_minus_start_test() {
  "XRANGE fruits - 1526985054079"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRange(
    stream: "fruits",
    start: command.Unspecified,
    end: command.Timestamp(1_526_985_054_079),
  ))
}

pub fn parse_xrange_plus_end_test() {
  "XRANGE fruits 1526985054069 +"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRange(
    stream: "fruits",
    start: command.Timestamp(1_526_985_054_069),
    end: command.Unspecified,
  ))
}

pub fn parse_xread_one_stream_test() {
  "XREAD STREAMS fruits 1526985054069-0"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRead(
    streams: [#("fruits", command.Explicit(1_526_985_054_069, 0))],
    block: option.None,
  ))
}

pub fn parse_xread_three_streams_test() {
  "XREAD STREAMS fruits shoes clock 1526985054069-0 1526986857397 $"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRead(
    streams: [
      #("fruits", command.Explicit(1_526_985_054_069, 0)),
      #("shoes", command.Timestamp(1_526_986_857_397)),
      #("clock", command.Unspecified),
    ],
    block: option.None,
  ))
}

pub fn parse_xread_block_1500_test() {
  "XREAD BLOCK 1500 STREAMS fruits 1526985054069-0"
  |> command_resp
  |> command.parse
  |> should.be_ok
  |> should.equal(command.XRead(
    streams: [#("fruits", command.Explicit(1_526_985_054_069, 0))],
    block: option.Some(1500),
  ))
}

fn command_resp(str: String) -> resp.Resp {
  string.split(str, on: " ")
  |> list.map(resp.BulkString)
  |> resp.Array
}
