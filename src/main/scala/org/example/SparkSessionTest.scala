package org.example
import org.apache.spark.sql.SparkSession

import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

/*
object SparkSessionTest extends App {

//  def main() {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("arpan-kafka-1")
      .getOrCreate()

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "arpan.quickstart.sampleData.topic")
      .load()

    df.printSchema()

    val df2 = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)", "topic")
    df2.show(false)

    //  val df = spark
    //    .readStream
    //    .format("kafka")
    //    .option("kafka.bootstrap.servers", "localhost:9092")
    //    .option("subscribe", "arpan.quickstart.sampleData.topic")
    //    .load()
    //  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //  df.writestream.format("csv").save("s3://data-pipeline-poc-hme/arpan-kafka.csv")
//  }
}

 */

//
//object SparkSessionTest extends App{
//
//  val props:Properties = new Properties()
//  props.put("group.id", "test2")
//  props.put("bootstrap.servers","localhost:9092")
//  props.put("key.deserializer",
//    "org.apache.kafka.common.serialization.StringDeserializer")
//  props.put("value.deserializer",
//    "org.apache.kafka.common.serialization.StringDeserializer")
//  props.put("enable.auto.commit", "true")
//  props.put("auto.commit.interval.ms", "1000")
////  props.put("startingOffsets", 0)
//  props.put("auto.offset.reset", "earliest") //latest, earliest, none
//  val consumer = new KafkaConsumer(props)
//  val topics = List("arpan.quickstart.sampleData.topic")
//  try {
//    consumer.subscribe(topics.asJava)
//    while (true) {
//      val records = consumer.poll(10)
//      for (record <- records.asScala) {
//        println("Topic: " + record.topic() +
//          ",Key: " + record.key() +
//          ",Value: " + record.value() +
//          ", Offset: " + record.offset() +
//          ", Partition: " + record.partition())
//      }
//    }
//  }catch{
//    case e:Exception => e.printStackTrace()
//  }finally {
//    consumer.close()
//  }
//}

object SparkSessionTest extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName)
  println("Deploy Mode :"+spark.sparkContext.deployMode)
  println("Master :"+spark.sparkContext.master)

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate()

  println("Second SparkContext:")
  println("APP Name :"+sparkSession2.sparkContext.appName)
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode)
  println("Master :"+sparkSession2.sparkContext.master)
}
