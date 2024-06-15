pub type RedisValue {
  String(String)
  Stream(List(StreamEntry))
  None
}

pub type StreamEntry =
  #(Int, Int, List(#(String, String)))

pub fn to_type_name(value: RedisValue) -> String {
  case value {
    String(_) -> "string"
    Stream(_) -> "stream"
    None -> "none"
  }
}
