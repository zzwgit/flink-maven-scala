package com.opensourceteams.module.bigdata.flink.example.datastream.connectors.kafka

import java.util.Properties

import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object ReadKafkaDataRun {

  def main(args: Array[String]): Unit = {

    val configuration: Configuration = ConfigurationUtil.getConfiguration(true)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, configuration)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.21.13.182:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "10.21.13.182:2181")
    properties.setProperty("group.id", "test")
    import org.apache.flink.streaming.api.scala._

    env.addSource(new FlinkKafkaConsumer[String]("lzztest", new SimpleStringSchema(), properties))
      .print().setParallelism(3)

    env.execute("读取kafka数据")

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long) {
    //override def toString: String = Thread.currentThread().getName + word + " : " + count
  }


}
