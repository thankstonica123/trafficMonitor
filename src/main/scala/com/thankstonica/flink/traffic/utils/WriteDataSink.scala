package com.thankstonica.flink.traffic.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class WriteDataSink[T](classType:Class[_<:T]) extends RichSinkFunction[T]{
  var conn :Connection =_
  var pst :PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost/test??useUnicode=true&characterEncoding=UTF-8","root","123")
    if(classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)){
      pst = conn.prepareStatement("insert into t_speeding_info (car,monitor_id,road_id,real_speed,limit_speed,action_time) values (?,?,?,?,?,?)")
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    if(classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)){
      val info: OutOfLimitSpeedInfo = value.asInstanceOf[OutOfLimitSpeedInfo]
      pst.setString(1,info.car)
      pst.setString(2,info.monitorId)
      pst.setString(3,info.roadId)
      pst.setDouble(4,info.realSpeed)
      pst.setInt(5,info.limitSpeed)
      pst.setLong(6,info.actionTime)
      pst.executeUpdate()
    }
  }


}
