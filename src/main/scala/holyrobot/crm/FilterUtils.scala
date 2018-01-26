package holyrobot.crm

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by cuill on 2018/1/22.
  */
object FilterUtils {
  def filterOrderVisitorByOrderId(fields: Array[String], orderIds: mutable.HashSet[String]): Boolean = {
    orderIds.contains(fields(1))
  }

  def filterOrderCostByOrderId(fields: Array[String], orderIds: mutable.HashSet[String]): Boolean = {
    if(fields.length == 4){
      println(fields)
    }
    orderIds.contains(fields(1))
  }

  def filterOrderIncomeByOrderId(fields: Array[String], orderIds: mutable.HashSet[String]): Boolean = {
    orderIds.contains(fields(1))
  }


  def process(routeschedules: RDD[Array[String]], routeIds: mutable.HashSet[String]) = {
    val maps: RDD[(String, Iterable[Array[String]])] = routeschedules.groupBy(_ (1))
    val RouteScheduleGB: RDD[Iterable[Array[String]]] = maps.map(_._2)
    RouteScheduleGB.foreach(arr => {

    })
  }

  def filterRouteScheuleByRouteId(fileds: Array[String], routeIds: mutable.HashSet[String]): Boolean = {
    routeIds.contains(fileds(1))
  }

  def filterOrderInfoByCustomerName(fields: Array[String], nameFileds: String): Boolean = {
    fields(12).contains(nameFileds)
  }

}
