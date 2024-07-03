import redis/stream.{type Stream}

pub type RedisValue {
  String(String)
  Integer(Int)
  Stream(Stream)
  None
}

pub fn to_type_name(value: RedisValue) -> String {
  case value {
    String(_) -> "string"
    Integer(_) -> "integer"
    Stream(_) -> "stream"
    None -> "none"
  }
}
