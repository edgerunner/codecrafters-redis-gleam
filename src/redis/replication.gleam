pub type Replication {
  Master(master_replid: String, master_repl_offset: Int)
}

pub fn master() -> Replication {
  Master(master_replid: random_replid(), master_repl_offset: 0)
}

import gleam/int
import gleam/iterator
import gleam/string

fn random_replid() -> String {
  fn() {
    case int.random(36) {
      num if num < 10 -> num + 0x30
      alpha -> alpha + 0x57
    }
  }
  |> iterator.repeatedly
  |> iterator.filter_map(string.utf_codepoint)
  |> iterator.take(40)
  |> iterator.to_list
  |> string.from_utf_codepoints
}
