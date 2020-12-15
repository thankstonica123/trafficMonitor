package com.thankstonica.flink.traffic.utils

import org.apache.flink.api.common.state.MapStateDescriptor

// 从kafka中读取数据，车辆经过卡口的信息
case class TrafficInfo(actionTime:Long,monitorId:String,cameraId:String,car:String,speed:Double,roadId:String,areaId:String)

// 卡口信息样例类
case class MonitorInfo(monitorId:String,roadId:String,speedLimit:Int,areaId:String)

// 车辆超速信息
case class OutOfLimitSpeedInfo(car:String,monitorId:String,roadId:String,realSpeed:Double,limitSpeed:Int,actionTime:Long)

object GlobalConstants {
  lazy val MONITOR_STATE_DESCRIPTOR =new MapStateDescriptor[String,MonitorInfo]("monitor_info",classOf[String],classOf[MonitorInfo])
  //lazy val VIOLATION_STATE_DESCRIPTOR =new MapStateDescriptor[String,ViolationInfo]("violation_info",classOf[String],classOf[ViolationInfo])

}
