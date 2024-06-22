import gleam/erlang/process
import gleam/otp/task
import gleeunit/should
import redis/command.{Explicit}
import redis/resp.{Array, BulkString}
import redis/stream

pub fn xrange_explicit_middle_test() {
  let stream = dummy_stream("xrange_explicit_middle")
  stream.handle_xrange(stream, Explicit(2, 0), Explicit(3, 0))
  |> should.equal(
    Array([
      Array([BulkString("2-0"), Array([BulkString("two"), BulkString("iki")])]),
      Array([
        BulkString("2-1"),
        Array([
          BulkString("two"),
          BulkString("deux"),
          BulkString("deux"),
          BulkString("dos"),
        ]),
      ]),
      Array([
        BulkString("3-0"),
        Array([BulkString("three"), BulkString("üç")]),
      ]),
    ]),
  )
}

pub fn xread_explicit_test() {
  let stream = dummy_stream("xread_explicit")
  stream.handle_xread(stream, Explicit(2, 1))
  |> should.equal(
    Array([
      Array([
        BulkString("3-0"),
        Array([BulkString("three"), BulkString("üç")]),
      ]),
      Array([
        BulkString("4-0"),
        Array([BulkString("four"), BulkString("dört")]),
      ]),
    ]),
  )
}

pub fn xread_block_before_last_test() {
  let stream = dummy_stream("xread_block_before_last")

  stream.handle_xread_block(stream, Explicit(3, 0), 1000)
  |> should.equal(
    Array([
      Array([
        BulkString("4-0"),
        Array([BulkString("four"), BulkString("dört")]),
      ]),
    ]),
  )
}

pub fn xread_block_test() {
  let stream = dummy_stream("xread_block")

  fn() {
    process.sleep(200)
    stream.handle_xadd(stream, Explicit(5, 0), [#("five", "beş")])
  }
  |> task.async

  stream.handle_xread_block(stream, Explicit(4, 0), 1000)
  |> should.equal(
    Array([
      Array([BulkString("5-0"), Array([BulkString("five"), BulkString("beş")])]),
    ]),
  )
}

pub fn xread_block_timeout_test() {
  let stream = dummy_stream("xread_block_timeout")

  fn() {
    process.sleep(200)
    stream.handle_xadd(stream, Explicit(5, 0), [#("five", "beş")])
  }
  |> task.async

  stream.handle_xread_block(stream, Explicit(4, 0), 100)
  |> should.equal(resp.Null(resp.NullString))
}

pub fn xread_block_no_timeout_test() {
  let stream = dummy_stream("xread_block_no_timeout")

  fn() {
    process.sleep(200)
    stream.handle_xadd(stream, Explicit(5, 0), [#("five", "beş")])
  }
  |> task.async

  stream.handle_xread_block(stream, Explicit(4, 0), 0)
  |> should.equal(
    Array([
      Array([BulkString("5-0"), Array([BulkString("five"), BulkString("beş")])]),
    ]),
  )
}

fn dummy_stream(name: String) {
  let stream = stream.new(name) |> should.be_ok
  stream.handle_xadd(stream, Explicit(1, 0), [#("one", "bir")])
  stream.handle_xadd(stream, Explicit(2, 0), [#("two", "iki")])
  stream.handle_xadd(stream, Explicit(2, 1), [
    #("two", "deux"),
    #("deux", "dos"),
  ])
  stream.handle_xadd(stream, Explicit(3, 0), [#("three", "üç")])
  stream.handle_xadd(stream, Explicit(4, 0), [#("four", "dört")])
  stream
}
