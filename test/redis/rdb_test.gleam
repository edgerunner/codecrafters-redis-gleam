import gleam/dict
import gleam/option
import gleeunit/should
import redis/rdb
import redis/resp

const rdb_file = <<
  // header, version
  "REDIS":utf8, "0011":utf8,
  // metadata redis-ver: 7.2.5
  0xfa, 0x09, "redis-ver":utf8, 0x05, "7.2.5":utf8,
  // metadata redis-bits: 64
  0xfa, 0x0a, "redis-bits":utf8, 0xc0, 0x40,
  // metadata ctime: 1_718_121_924
  0xfa, 0x05, "ctime":utf8, 0xc2, 1_718_121_924:size(32)-little,
  // metadata used-mem: 1_197_984
  0xfa, 0x08, "used-mem":utf8, 0xc2, 1_197_984:size(32)-little,
  // metadata aof-base: 0
  0xfa, 0x08, "aof-base":utf8, 0xc0, 0x00,
  // database 0, resize hash:2 expire:0
  0xfe, 0x00, 0xfb, 0x02, 0x00,
  // string: haskell => curry
  0x00, 0x07, "haskell":utf8, 0x05, "curry":utf8,
  // string: foo => bar
  0x00, 0x03, "foo":utf8, 0x03, "bar":utf8,
  // EOF, checksum
  0xff, 0xa1f1_f3df_3053_8d0f:big,
>>

pub fn parse_header_test() {
  let rdb =
    rdb.parse(rdb_file)
    |> should.be_ok
  rdb.version |> should.equal("0011")
}

pub fn parse_metadata_test() {
  let rdb =
    rdb.parse(rdb_file)
    |> should.be_ok
  dict.get(rdb.metadata, "redis-ver")
  |> should.be_ok
  |> should.equal("7.2.5")
  dict.get(rdb.metadata, "redis-bits")
  |> should.be_ok
  |> should.equal("64")
  dict.get(rdb.metadata, "ctime")
  |> should.be_ok
  |> should.equal("1718121924")
}

import gleam/io

pub fn parse_key_string_value_in_db_test() {
  let rdb =
    rdb.parse(rdb_file)
    |> io.debug
    |> should.be_ok
  let db =
    dict.get(rdb.databases, 0)
    |> should.be_ok
  dict.get(db, "haskell")
  |> should.be_ok
  |> should.equal(#("haskell", resp.BulkString("curry"), option.None))
  dict.get(db, "foo")
  |> should.be_ok
  |> should.equal(#("foo", resp.BulkString("bar"), option.None))
}
