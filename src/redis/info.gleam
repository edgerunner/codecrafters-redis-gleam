import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import redis/resp.{type Resp}

pub fn handle_replication(replicaof: Option(#(String, Int))) -> Resp {
  [#("role", to_role(replicaof))]
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
