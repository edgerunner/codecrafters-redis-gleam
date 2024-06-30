import gleam/erlang/process.{type Subject}
import gleam/otp/actor

/// Start a counter that only counts
/// the values that pass the `predicate`.
/// It also returns early if the `timeout` (in ms) or
/// the `target` is reached.
///
/// Returns a 2-tuple of `Subject`s: `#(count, done)`.
/// Send `count` your value to have it evaluated against
/// the predicate and (maybe) counted. Listen on `done`
/// to receive the result when the counter resolves.
pub fn start(
  until target: Int,
  for timeout: Int,
  counting predicate: fn(a) -> Bool,
) -> #(Subject(a), Subject(Int)) {
  let target_minus_one = target - 1
  let count_subject_ready = process.new_subject()
  let done = process.new_subject()
  let assert Ok(_) =
    actor.Spec(
      init_timeout: 100,
      init: fn() {
        let timed_out = process.new_subject()
        let count_request = process.new_subject()
        let selector =
          process.new_selector()
          |> process.selecting(timed_out, fn(_) { TimedOut })
          |> process.selecting(count_request, fn(a) {
            predicate(a) |> CountRequested
          })
        actor.send(count_subject_ready, count_request)
        process.send_after(timed_out, timeout, Nil)
        actor.Ready(0, selector)
      },
      loop: fn(msg: Msg, count: Int) {
        case msg {
          TimedOut -> {
            process.send(done, count)
            actor.Stop(process.Normal)
          }
          CountRequested(False) -> actor.continue(count)
          CountRequested(True) if count >= target_minus_one -> {
            process.send(done, count + 1)
            actor.Stop(process.Normal)
          }
          CountRequested(True) -> actor.continue(count + 1)
        }
      },
    )
    |> actor.start_spec

  let assert Ok(count_subject) = process.receive(count_subject_ready, 100)
  #(count_subject, done)
}

type Msg {
  CountRequested(Bool)
  TimedOut
}
