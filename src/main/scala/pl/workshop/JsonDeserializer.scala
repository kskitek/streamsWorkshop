package pl.workshop

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

private object JsonDeserializer {
  val objectMapper = new ObjectMapper
}

class JsonDeserializer[T](clazz: Class[T]) extends Deserializer[T] {
  override def deserialize(topic: String, data: Array[Byte]): T =
  // TODO can this happen?
  //    if (data == null) {
  //      null
  //    } else {
    try {
      JsonDeserializer.objectMapper.readValue(data, clazz)
    } catch {
      case e: Exception => throw new SerializationException(e)
    }

  //    }
}
