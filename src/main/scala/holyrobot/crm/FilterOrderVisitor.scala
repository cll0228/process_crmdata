package holyrobot.crm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by cuill on 2018/1/24.
  */
object FilterOrderVisitor {
  def main(args: Array[String]): Unit = {

    if (args.length <= 0) {
      println("请传入参数！！")
      return
    }

    val conf = new SparkConf().setAppName("FilterOrderIncomeAndCost")

    if (args.length == 0) {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    //加载订单和订单分列项信息：收入和支出
    val orderInfo = sc.textFile(args(0)).map(_.split("\\|"))
    val ordervisitor = sc.textFile(args(1)).map(_.split("\\|"))

    //        val orderInfo = sc.textFile("hdfs://node2:8020/crmdata/sqlserver/orderinfo/part*").map(_.split("\\|"))
    //        val ordervisitor = sc.textFile("hdfs://node2:8020/crmdata/sqlserver/ordervisitor/part*").map(_.split("\\|"))

    //订单表根据携程过滤
    val filterOrderInfo: RDD[Array[String]] = orderInfo.filter(fileds => FilterUtils.filterOrderInfoByCustomerName(fileds, FilterRouteName.CRTIP))

    //携程订单Id
    val orderIds = new mutable.HashSet[String]()

    filterOrderInfo.collect().map(line => {
      orderIds += line(0)
    })

    val filterOrderVisitor: RDD[Array[String]] = ordervisitor.filter(fields => FilterUtils.filterOrderVisitorByOrderId(fields, orderIds))

    //SAVE
    //        filterOrderVisitor.map(_.mkString("|")).saveAsTextFile("hdfs://node2:8020/crmdata/sqlserver/result/ordervisitor")
    filterOrderVisitor.map(_.mkString("|")).saveAsTextFile(args(2))
    sc.stop()
  }
}
