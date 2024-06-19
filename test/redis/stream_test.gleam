import gleeunit/should
import redis/command.{Explicit}
import redis/resp.{Array, BulkString}
import redis/stream

pub fn xrange_explicit_middle_test() {
  dummy_stream()
  |> stream.handle_xrange(Explicit(2, 0), Explicit(3, 0))
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

fn dummy_stream() {
  let stream = stream.new("dummy") |> should.be_ok
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
