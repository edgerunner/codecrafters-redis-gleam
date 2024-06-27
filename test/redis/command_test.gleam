import gleam/option.{None, Some}
import gleam/set
import gleeunit/should
import redis/command
import redis/config
import redis/resp

pub fn parse_ping_test() {
  "PING"
  |> should_parse_into(command.Ping)
}

pub fn parse_ping_mixed_case_test() {
  "PinG"
  |> should_parse_into(command.Ping)
}

pub fn parse_echo_hello_test() {
  "ECHO hello"
  |> should_parse_into(command.Echo(resp.BulkString("hello")))
}

pub fn fail_parse_echo_no_argument_test() {
  "ECHO"
  |> command_resp
  |> command.parse
  |> should.be_error
  |> should.equal(command.WrongNumberOfArguments)
}

pub fn parse_set_key_value_test() {
  "SET key value"
  |> should_parse_into(command.Set(
    key: "key",
    value: "value",
    expiry: option.None,
  ))
}

pub fn parse_set_key_value_with_expiry_test() {
  "SET key value EX 15"
  |> should_parse_into(command.Set(
    key: "key",
    value: "value",
    expiry: option.Some(15_000),
  ))
}

pub fn parse_set_key_value_with_precise_expiry_test() {
  "SET key value PX 125"
  |> should_parse_into(command.Set(
    key: "key",
    value: "value",
    expiry: option.Some(125),
  ))
}

pub fn parse_get_key_test() {
  "GET key"
  |> should_parse_into(command.Get("key"))
}

pub fn fail_parse_get_too_many_arguments_test() {
  "GET key value EX 15"
  |> command_resp
  |> command.parse
  |> should.be_error
  |> should.equal(command.WrongNumberOfArguments)
}

pub fn parse_config_get_dir_test() {
  "CONFIG GET dir"
  |> should_parse_into(command.Config(command.ConfigGet(config.Dir)))
}

pub fn parse_config_get_dbfilename_test() {
  "CONFIG GET dbfilename"
  |> should_parse_into(command.Config(command.ConfigGet(config.DbFilename)))
}

pub fn parse_keys_star_test() {
  "KEYS *"
  |> should_parse_into(command.Keys(option.None))
}

pub fn parse_type_test() {
  "TYPE flagon"
  |> should_parse_into(command.Type("flagon"))
}

pub fn parse_xadd_explicit_test() {
  "XADD fruits 12345678-0 mango 5 kiwi 48 apple 12"
  |> should_parse_into(
    command.XAdd(stream: "fruits", id: command.Explicit(12_345_678, 0), data: [
      #("mango", "5"),
      #("kiwi", "48"),
      #("apple", "12"),
    ]),
  )
}

pub fn parse_xadd_auto_sequence_test() {
  "XADD fruits 12345678-* mango 15 apple 12"
  |> should_parse_into(
    command.XAdd(stream: "fruits", id: command.Timestamp(12_345_678), data: [
      #("mango", "15"),
      #("apple", "12"),
    ]),
  )
}

pub fn parse_xadd_auto_test() {
  "XADD fruits * banana none mango 15 apple 12"
  |> should_parse_into(
    command.XAdd(stream: "fruits", id: command.Unspecified, data: [
      #("banana", "none"),
      #("mango", "15"),
      #("apple", "12"),
    ]),
  )
}

pub fn parse_xrange_explicit_test() {
  "XRANGE fruits 1526985054069-0 1526985054079-5"
  |> should_parse_into(command.XRange(
    stream: "fruits",
    start: command.Explicit(1_526_985_054_069, 0),
    end: command.Explicit(1_526_985_054_079, 5),
  ))
}

pub fn parse_xrange_timestamp_test() {
  "XRANGE fruits 1526985054069 1526985054079"
  |> should_parse_into(command.XRange(
    stream: "fruits",
    start: command.Timestamp(1_526_985_054_069),
    end: command.Timestamp(1_526_985_054_079),
  ))
}

pub fn parse_xrange_minus_start_test() {
  "XRANGE fruits - 1526985054079"
  |> should_parse_into(command.XRange(
    stream: "fruits",
    start: command.Unspecified,
    end: command.Timestamp(1_526_985_054_079),
  ))
}

pub fn parse_xrange_plus_end_test() {
  "XRANGE fruits 1526985054069 +"
  |> should_parse_into(command.XRange(
    stream: "fruits",
    start: command.Timestamp(1_526_985_054_069),
    end: command.Unspecified,
  ))
}

pub fn parse_xread_one_stream_test() {
  "XREAD STREAMS fruits 1526985054069-0"
  |> should_parse_into(command.XRead(
    streams: [#("fruits", command.Explicit(1_526_985_054_069, 0))],
    block: option.None,
  ))
}

pub fn parse_xread_three_streams_test() {
  "XREAD STREAMS fruits shoes clock 1526985054069-0 1526986857397 $"
  |> should_parse_into(command.XRead(
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
  |> should_parse_into(command.XRead(
    streams: [#("fruits", command.Explicit(1_526_985_054_069, 0))],
    block: option.Some(1500),
  ))
}

pub fn parse_info_replication_test() {
  "INFO replication"
  |> should_parse_into(command.Info(command.InfoReplication))
}

pub fn parse_replconf_listening_port_test() {
  "REPLCONF listening-port 5432"
  |> should_parse_into(command.ReplConf(command.ReplConfListeningPort(5432)))
}

pub fn parse_replconf_capa_test() {
  "REPLCONF capa eof capa psync2"
  |> should_parse_into(
    command.ReplConf(command.ReplConfCapa(set.from_list(["eof", "psync2"]))),
  )
}

pub fn parse_initial_psync_test() {
  "PSYNC ? -1"
  |> should_parse_into(command.PSync(id: None, offset: -1))
}

pub fn parse_subsequent_psync_test() {
  "PSYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 5"
  |> should_parse_into(command.PSync(
    id: Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
    offset: 5,
  ))
}

pub fn parse_replconf_getack_wildcard_test() {
  "REPLCONF GETACK *"
  |> should_parse_into(command.ReplConf(command.ReplConfGetAck(None)))
}

pub fn parse_replconf_getack_with_offset_test() {
  "REPLCONF GETACK 456"
  |> should_parse_into(command.ReplConf(command.ReplConfGetAck(Some(456))))
}

pub fn parse_wait_0_60000_test() {
  "WAIT 0 60000"
  |> should_parse_into(command.Wait(replicas: 0, timeout: 60_000))
}

// Helpers

import gleam/list
import gleam/string

fn should_parse_into(input: String, expected: command.Command) {
  command_resp(input)
  |> command.parse
  |> should.be_ok
  |> should.equal(expected)
}

fn command_resp(str: String) -> resp.Resp {
  string.split(str, on: " ")
  |> list.map(resp.BulkString)
  |> resp.Array
}
