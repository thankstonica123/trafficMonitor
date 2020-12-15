package com.thankstonica.flink.traffic.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class JdbcReadDataSource[T](classType:Class[_<:T])  extends RichSourceFunction[T]{
  var flag =true;
  var conn :Connection =_
  var pst :PreparedStatement = _
  var set :ResultSet = _


  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost/test??useUnicode=true&characterEncoding=UTF-8","root","123")
    if(classType.getName.equals(classOf[MonitorInfo].getName)){
      pst = conn.prepareStatement("select monitor_id,road_id,speed_limit,area_id from t_monitor_info where speed_limit > 0")
    }

  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    while (flag){
      set =pst.executeQuery()
      while (set.next()){
        if(classType.getName.equals(classOf[MonitorInfo].getName)){
          var info =new MonitorInfo(set.getString(1),set.getString(2),set.getInt(3),set.getString(4))
          ctx.collect(info.asInstanceOf[T])
        }
      }
      Thread.sleep(2000)
      set.close()
    }
  }

  override def cancel(): Unit = {
    flag = false
  }

  override def close(): Unit = {
    pst.close()
    conn.close()

  }
}

