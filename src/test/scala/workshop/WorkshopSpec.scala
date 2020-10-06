package workshop

import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util.{Date, Properties}

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.slf4j.LoggerFactory
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream._

class WorkshopSpec extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {

  import org.apache.kafka.streams.scala.Serdes._

  var currentTopologyDriver: Option[TopologyTestDriver] = None
  var printTopology = false

  override def afterAll(): Unit = if (currentTopologyDriver.nonEmpty) {
    currentTopologyDriver.get.close()
    currentTopologyDriver = None
  }

  def getTestDriver(sb: StreamsBuilder): TopologyTestDriver = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)

    val topology = sb.build()
    if (printTopology) {
      val logger = LoggerFactory.getLogger(classOf[WorkshopSpec])
      logger.info("Paste topology to https://zz85.github.io/kafka-streams-viz/")
      logger.info(topology.describe().toString)
    }
    val driver = new TopologyTestDriver(topology, props)
    currentTopologyDriver = Some(driver)
    driver
  }

  "Kafka Streams" should "be easy" in {
    val builder: StreamsBuilder = new StreamsBuilder

    val sb = new StreamsBuilder
    sb.stream[String, String]("testTopic")
      .to("testOutputTopic")

    val td = getTestDriver(sb)
    val inputTopic = td.createInputTopic("testTopic", Serdes.String().serializer(), Serdes.String().serializer())
    inputTopic.pipeInput("key", "value")

    val outputTopic = td.createOutputTopic("testOutputTopic", Serdes.String().deserializer(), Serdes.String().deserializer())

    val kv = outputTopic.readKeyValue()
    "key" should be(kv.key)
  }

  it should "be in logs" in {
    // println any message that passes through the stream
    // - can you do that in more than one way?
  }

  it should "be positive" in {
    // return only positive numbers

    val sb = new StreamsBuilder

    val td = getTestDriver(sb)
    val in = td.createInputTopic("testTopic", Serdes.String().serializer(), Serdes.Integer().serializer())
    in.pipeInput("key", 1)
    in.pipeInput("key", -1)
    in.pipeInput("key", 2)
    in.pipeInput("key", -2)
    in.pipeInput("key", 3)
    in.pipeInput("key", -3)

    val out = td.createOutputTopic("testOutputTopic", Serdes.String().deserializer(), Serdes.Integer().deserializer())

    assert(3 == out.getQueueSize)
    assert(1 == out.readValue())
    assert(2 == out.readValue())
    assert(3 == out.readValue())
  }

  it should "add numbers with no sweat" in {
    // return sum of all values in the stream
  }

  it should "not add apples to oranges" in {
    // return sum of apples and sum of oranges separately

    val sb = new StreamsBuilder

    val td = getTestDriver(sb)
    val in = td.createInputTopic("testTopic", Serdes.String.serializer, Serdes.Integer().serializer)
    in.pipeInput("apple", 1)
    in.pipeInput("orange", 1)
    in.pipeInput("apple", 9)
    in.pipeInput("orange", 1)
    in.pipeInput("orange", 1)

    var out = td.createOutputTopic("testOutputTopic", Serdes.String().deserializer(), Serdes.Integer().deserializer())
  }

  it should "never be bored of counting" in {
    // return sum of apples and sum of oranges to separate topics

    val sb = new StreamsBuilder

    val td = getTestDriver(sb)
    var apples = td.createOutputTopic("apples", Serdes.String.deserializer, Serdes.Integer.deserializer)
    var oranges = td.createOutputTopic("oranges", Serdes.String.deserializer, Serdes.Integer.deserializer)
  }

  it should "count WTF's per minute" in {
    val sb = new StreamsBuilder

    val td = getTestDriver(sb)
    val in = td.createInputTopic("testTopic", Serdes.String().serializer(), Serdes.String().serializer())
    val zeroTime = LocalDateTime.of(2222, 2, 2, 22, 0).toInstant(ZoneOffset.UTC)
    val minute = Duration.ofMinutes(1)

    in.pipeInput("John", "wtf", zeroTime)
    in.pipeInput("John", "wtf", zeroTime)
    in.pipeInput("Doe", "wtf", zeroTime)
    in.pipeInput("Doe", "wtf", zeroTime)
    in.pipeInput("Doe", "wtf", zeroTime)
    in.pipeInput("Jane", "wtf", zeroTime.plus(minute))
    in.pipeInput("Travis", "wtf", zeroTime.plus(minute))
    in.pipeInput("Skitek", "wtf", zeroTime.plus(minute))
    in.pipeInput("Skitek", "wtf", zeroTime.plus(minute.multipliedBy(2)))
    in.pipeInput("Skitek", "wtf", zeroTime.plus(minute.multipliedBy(2)))
    in.pipeInput("Mr. Jenkins", "it's all good", zeroTime)
  }

  it should "know good or evil" in {
    // given keys in format {prefix}_{key} return count of events per key
    // see how your changes influence topology.
    printTopology = true
    // see the logs of a test for printed topology. You can visualize it here: https://zz85.github.io/kafka-streams-viz/

    val sb = new StreamsBuilder

    val td = getTestDriver(sb)
    val in = td.createInputTopic("testTopic", Serdes.String().serializer(), Serdes.String().serializer())
    in.pipeInput("very_bad", "evil! evil! evil!")
    in.pipeInput("not really but trying to be_bad", "maybe evil, maybe not")
    in.pipeInput("very_good", "trying to be")
  }

  it should "play nicely with data points" in {
    // calculate sum od data per minute per deviceId
    // return only double value
    val sb = new StreamsBuilder

    val td = getTestDriver(sb)
    val in: TestInputTopic[String, DataPoint] = null
    val zeroTime = LocalDateTime.of(2222, 2, 2, 22, 0).toInstant(ZoneOffset.UTC)
    in.pipeInput("id1", DataPoint(new Date(zeroTime.toEpochMilli), 1d), zeroTime)

    var sums = td.createOutputTopic("sums", Serdes.String().deserializer(), Serdes.Double().deserializer())
  }

  it should "get physical" in {
    // calculate sum per 30 days of data in dataPoints per deviceId
    // return data point
  }
}
