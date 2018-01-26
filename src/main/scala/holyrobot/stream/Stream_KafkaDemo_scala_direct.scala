package holyrobot.stream

import kafka.serializer.StringDecoder
import log.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 区别于第一种方式：
  * 1、offset不会更新到zookeeper
  * 2、使用的节点端口是9092kafka的broker端口
  * 3、不存在group
  */
object Stream_KafkaDemo_scala_direct {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()

    //接收命令行中的参数,提交集群模式接受参数
    // val Array(broker.list) = args

    //获取sparkstreaming
    val ssc = new StreamingContext(new SparkConf().setAppName("streamingkafka").setMaster("local[2]"), Seconds(5))
    //创建kafkaParams
    val kafkaParams = Array("metadata.broker.list").map {(_, "min1:9092,min2:9092,min3:9092")}.toMap

    //创建topic
    val topics = Array("par111").toSet

    //获取lines
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    lines.print()//处理业务逻辑

    ssc.start()
    ssc.awaitTermination() //一直等待生产者生产消息
  }
}
