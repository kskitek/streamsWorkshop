package pl.workshop

import org.apache.kafka.common.serialization.{Serde, Serdes}

object CustomSerdeFactory {
  def getJson[T](clazz: Class[T]): Serde[T] = Serdes.serdeFrom(new JsonSerializer[T], new JsonDeserializer[T](clazz))
}
