package holyrobot.crm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cuill on 2018/1/24.
  */
object FilterOrderInfo {
  def main(args: Array[String]): Unit = {

    if (args.length <= 0) {
      println("请传入参数！！")
      return
    }

    val conf = new SparkConf().setAppName("FilterRouteSchedule")

    if (args.length == 0) {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    //加载订单和路径信息
    //        val orderInfo = sc.textFile("hdfs://node2:8020/crmdata/sqlserver/orderinfo/part*").map(_.split("\\|"))
    val orderInfo = sc.textFile(args(0)).map(_.split("\\|"))
    //订单表根据携程过滤
    val filterOrderInfo: RDD[Array[String]] = orderInfo.filter(fileds => FilterUtils.filterOrderInfoByCustomerName(fileds, FilterRouteName.CRTIP))

    //        var endResult = filterOrderInfo.map(_.mkString("|")).saveAsTextFile("hdfs://node2:8020/crmdata/sqlserver/result/orderinfo")
    var endResult = filterOrderInfo.map(_.mkString("|")).saveAsTextFile(args(1))
    sc.stop()
  }
}
