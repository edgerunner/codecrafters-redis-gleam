pub type RedisValue {
  String(String)
  Stream(List(#(String, List(#(String, String)))))
  None
}

pub fn to_type_name(value: RedisValue) -> String {
  case value {
    String(_) -> "string"
    Stream(_) -> "stream"
    None -> "none"
  }
}
