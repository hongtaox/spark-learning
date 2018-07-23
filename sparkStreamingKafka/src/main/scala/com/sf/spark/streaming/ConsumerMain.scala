package com.sf.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object ConsumerMain {
  @transient lazy val log = LogManager.getRootLogger

  def createContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer").setMaster("local[4]")
      .set("spark.local.dir", "~/tmp")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    ssc.checkpoint("file:///E:/tmp/spark/test")
    // Create direct kafka stream with brokers and topics
    val topicsSet = "SHIVA_OMS_CORE_OPERATION_WAYBILL".split(",").toSet
    val brokers = "10.202.24.5:9095,10.202.24.6:9095,10.202.24.7:9095"
    val kafkaParams = scala.collection.immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
              "auto.offset.reset" -> "smallest",
              "group.id" -> "yourGroup")
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")

    //    kafkaDirectStream.cache

    val words = kafkaDirectStream.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(30), 2)
    wordCounts.print()

    //do something......

    //更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = createContext()
    ssc.start()
    ssc.awaitTermination()
  }
}
