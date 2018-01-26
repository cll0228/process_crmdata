package holyrobot.stream

import log.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 测试：启动kafka。创建一个topic
  *     程序作为消费者消费消息
  */
object Stream_KafkaDemo_scala {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()

    //接收命令行中的参数,提交集群模式接受参数
    // val Array(zkQuorum, groupId, topics, numThreads, hdfs) = args

    val conf = new SparkConf().setAppName("Stream_KafkaDemo_scala")
    //本地测试环境开启
    conf.setMaster("local[2]")

    //初始化ssc，设置间隔拉取时间
    val ssc = new StreamingContext(conf, Seconds(5))

    //实际环境参数由args传入
    val Array(zkQuorum, groupId, topic, numThreads) = Array[String]("min1:2181,min2:2181,min3:2181", "group2", "test0806", "1")
    val topicMap = topic.split(",").map((_, numThreads.toInt)).toMap

    var stream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    var lines = stream.map(_._2)

    val result: DStream[String] = lines.flatMap(_.split(" "))
    result.print() //拿到lines处理业务逻辑

    ssc.start()
    ssc.awaitTermination() //一直等待生产者生产消息
  }
}
