package pl.workshop

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer


private object JsonSerializer {
  val objectMapper = new ObjectMapper
}

class JsonSerializer[T] extends Serializer[T] {
  override def serialize(topic: String, data: T): Array[Byte] =
    if (data == null) {
      null
    } else {
      try {
        JsonSerializer.objectMapper.writeValueAsBytes(data)
      } catch {
        case e: Exception => throw new SerializationException("Error serializing JSON message", e)
      }
    }
}
