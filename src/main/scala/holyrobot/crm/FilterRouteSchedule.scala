package holyrobot.crm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by cuill on 2018/1/22.
  */
object FilterRouteSchedule {
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
    //    val orderInfo = sc.textFile("hdfs://node2:8020/crmdata/sqlserver/orderinfo/part*").map(_.split("\\|"))
    //    val routeschedule = sc.textFile("hdfs://node2:8020/crmdata/sqlserver/routeschedule/part*").map(_.split("\\|"))
    val orderInfo = sc.textFile(args(0)).map(_.split("\\|"))
    val routeschedule = sc.textFile(args(1)).map(_.split("\\|"))
    //订单表根据携程过滤
    val filterOrderInfo: RDD[Array[String]] = orderInfo.filter(fileds => FilterUtils.filterOrderInfoByCustomerName(fileds, FilterRouteName.CRTIP))

    //携程路径的 路径id 集合
    val routeIds = new mutable.HashSet[String]()
    //携程订单Id
    val orderIds = new mutable.HashSet[String]()

    //如果路径id为{00000000-0000-0000-0000-000000000000} 用id填充
    filterOrderInfo.collect().map(line => {
      orderIds += line(0)
      if (line(2).contains("0000")) {
        routeIds += line(0)
      } else {
        routeIds += line(2)
      }
    })

    //广播大变量
    sc.broadcast(routeIds)

    //找出携程的路径
    val result: RDD[Array[String]] = routeschedule.filter(fields => FilterUtils.filterRouteScheuleByRouteId(fields, routeIds))


    //    FilterUtils.process(result, routeIds)

    //save
    var endResult = result.map(_.mkString("|")).saveAsTextFile(args(2))
    //    var endResult = result.map(_.mkString("|")).saveAsTextFile("hdfs://node2:8020/crmdata/sqlserver/result/routeschedule")
    sc.stop()
  }
}


