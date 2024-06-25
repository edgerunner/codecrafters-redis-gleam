import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import redis/replication.{type Replication}
import redis/resp.{type Resp}

pub fn handle_replication(
  replicaof: Option(#(String, Int)),
  state: Replication,
) -> Resp {
  [
    #("role", to_role(replicaof)),
    #("master_replid", state.master_replid),
    #("master_repl_offset", "0"),
  ]
  |> to_resp
}

fn to_resp(kvs: List(#(String, String))) -> Resp {
  list.map(kvs, fn(kv) { kv.0 <> ":" <> kv.1 })
  |> list.reduce(fn(l, r) { l <> "\n" <> r })
  |> result.map(resp.BulkString)
  |> result.unwrap(resp.Null(resp.NullString))
}

fn to_role(replicaof: Option(a)) -> String {
  case replicaof {
    Some(_) -> "slave"
    None -> "master"
  }
}
