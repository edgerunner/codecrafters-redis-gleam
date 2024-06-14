import gleam/dict
import gleam/option
import gleeunit/should
import redis/rdb
import redis/value

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
  // expire timestamp seconds
  0xfd, 1_718_377_200:size(32)-little,
  // string past => tense
  0x00, 0x04, "past":utf8, 0x05, "tense":utf8,
  // expire timestamp miliseconds
  0xfc, 2_033_910_084_000:size(64)-little,
  // string future => perfect
  0x00, 0x06, "future":utf8, 0x07, "perfect":utf8,
  // EOF, no checksum, LF
  0xff, 0x0000000000000000:big-size(64), 0x0a,
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

pub fn parse_key_string_value_in_db_test() {
  let rdb =
    rdb.parse(rdb_file)
    |> should.be_ok
  let db =
    dict.get(rdb.databases, 0)
    |> should.be_ok
  dict.get(db, "haskell")
  |> should.be_ok
  |> should.equal(#("haskell", value.String("curry"), option.None))
  dict.get(db, "foo")
  |> should.be_ok
  |> should.equal(#("foo", value.String("bar"), option.None))
}

pub fn parse_expiring_key_string_value_in_db_test() {
  let rdb =
    rdb.parse(rdb_file)
    |> should.be_ok
  let db =
    dict.get(rdb.databases, 0)
    |> should.be_ok
  dict.get(db, "past")
  |> should.be_ok
  |> should.equal(#(
    "past",
    value.String("tense"),
    option.Some(1_718_377_200_000),
  ))
  dict.get(db, "future")
  |> should.be_ok
  |> should.equal(#(
    "future",
    value.String("perfect"),
    option.Some(2_033_910_084_000),
  ))
}
