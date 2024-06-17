import redis/stream.{type Stream}

pub type RedisValue {
  String(String)
  Stream(Stream)
  None
}

pub fn to_type_name(value: RedisValue) -> String {
  case value {
    String(_) -> "string"
    Stream(_) -> "stream"
    None -> "none"
  }
}
