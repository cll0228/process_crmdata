package holyrobot.crm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by cuill on 2018/1/24.
  * 过滤携程订单的收入和支出分列项
  */
object FilterOrderIncomeAndCost {
  def main(args: Array[String]): Unit = {
//    if (args.length <4) {
//      println("请传入参数！！")
//      return
//    }
    val conf = new SparkConf().setAppName("FilterOrderIncomeAndCost")
    if (args.length == 0) {
      conf.setMaster("local[1]")
    }
    val sc = new SparkContext(conf)

    //加载订单和订单分列项信息：收入和支出
        val orderInfo = sc.textFile("hdfs://node2:8020/crmdata/2018-01-25/sqlserver/orderinfo/part*").map(_.split("\\|"))
        val orderincome = sc.textFile("hdfs://node2:8020/crmdata/2018-01-25/sqlserver/orderincome/part*").map(_.split("\\|"))
        val ordercost = sc.textFile("hdfs://node2:8020/crmdata/2018-01-25/sqlserver/ordercost/part*").map(_.split("\\|"))
//    val orderInfo = sc.textFile(args(0)).map(_.split("\\|"))
//    val orderincome = sc.textFile(args(1)).map(_.split("\\|"))
//    val ordercost = sc.textFile(args(2)).map(_.split("\\|"))

    //订单表根据携程过滤
    val filterOrderInfo: RDD[Array[String]] = orderInfo.filter(fileds => FilterUtils.filterOrderInfoByCustomerName(fileds, FilterRouteName.CRTIP))

    //携程订单Id
    val orderIds = new mutable.HashSet[String]()

    filterOrderInfo.collect().map(line => {
      orderIds += line(0)
    })

    //广播大变量
    sc.broadcast(orderIds)

    //过滤出订单income , cost
    val filterOrderIncome: RDD[Array[String]] = orderincome.filter(fields => FilterUtils.filterOrderIncomeByOrderId(fields, orderIds))
    val filterOrdercost: RDD[Array[String]] = ordercost.filter(fields => FilterUtils.filterOrderCostByOrderId(fields, orderIds))

    //保存结果
        filterOrderIncome.map(_.mkString("|")).saveAsTextFile("hdfs://node2:8020/crmdata/sqlserver/result/orderincome")
        filterOrdercost.map(_.mkString("|")).saveAsTextFile("hdfs://node2:8020/crmdata/sqlserver/result/ordercost")
//    filterOrderIncome.map(_.mkString("|")).saveAsTextFile(args(3))
//    filterOrdercost.map(_.mkString("|")).saveAsTextFile(args(4))
    sc.stop()
  }
}
