import counter
import gleam/erlang/process
import gleeunit/should

pub fn counter_times_out_test() {
  let #(_count, done) =
    counter.start(until: 1, for: 100, counting: fn(_) { True })
  process.receive(done, 120)
  |> should.be_ok
  |> should.equal(0)
}

pub fn counter_times_out_after_counting_test() {
  let #(count, done) =
    counter.start(until: 5, for: 100, counting: fn(_) { True })
  process.send_after(count, 30, Nil)
  process.send_after(count, 70, Nil)
  process.receive(done, 120)
  |> should.be_ok
  |> should.equal(2)
}

pub fn counter_is_done_if_target_is_reached_early_test() {
  let #(count, done) =
    counter.start(until: 3, for: 200, counting: fn(_) { True })
  process.send_after(count, 30, Nil)
  process.send_after(count, 70, Nil)
  process.send_after(count, 80, Nil)
  process.receive(done, 100)
  |> should.be_ok
  |> should.equal(3)
}

pub fn counter_only_accepts_results_that_pass_the_predicate_test() {
  let #(count, done) =
    counter.start(until: 3, for: 100, counting: fn(bool) { bool })
  process.send_after(count, 30, True)
  process.send_after(count, 70, False)
  process.send_after(count, 80, True)
  process.receive(done, 110)
  |> should.be_ok
  |> should.equal(2)
}

pub fn counter_ignores_results_after_target_is_reached_test() {
  let #(count, done) =
    counter.start(until: 2, for: 100, counting: fn(bool) { bool })
  process.send_after(count, 30, True)
  process.send_after(count, 70, True)
  process.send_after(count, 80, True)
  process.receive(done, 110)
  |> should.be_ok
  |> should.equal(2)
}

pub fn counter_ignores_results_after_it_times_out_test() {
  let #(count, done) =
    counter.start(until: 5, for: 100, counting: fn(bool) { bool })
  process.send_after(count, 30, True)
  process.send_after(count, 70, True)
  process.send_after(count, 120, True)
  process.receive(done, 110)
  |> should.be_ok
  |> should.equal(2)
}
