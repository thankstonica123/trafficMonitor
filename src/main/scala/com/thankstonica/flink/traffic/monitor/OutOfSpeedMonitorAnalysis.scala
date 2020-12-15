package com.thankstonica.flink.traffic.monitor

import java.util.Properties

import com.thankstonica.flink.traffic.utils.{GlobalConstants, JdbcReadDataSource, MonitorInfo, OutOfLimitSpeedInfo, TrafficInfo, WriteDataSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

object OutOfSpeedMonitorAnalysis {
  def main(args: Array[String]): Unit = {
    val streamEnv:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //定义Kafka的连接属性
    val props = new Properties()
    props.setProperty("bootstrap.servers","s1:9092,s2:9092,s3:9092")
    props.setProperty("group.id","traffic01")
    /*props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")*/

    val stream1 = streamEnv.addSource(
      new FlinkKafkaConsumer[String]("t_traffic", new SimpleStringSchema(), props).setStartFromEarliest()
    ).map( line => {
      var arr: Array[String] = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })

    /*val stream = streamEnv.socketTextStream("s1", 9999).map(line => {
      var arr: Array[String] = line.split(",")
      new TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
    })*/

    //广播状态的操作步骤： 1 读取广播状态数据的Source（addSource） ，
    //2 把读取的数据广播出去
    //3 调用主流.connect算子，再调用Process底层API
    //4 定义一个底层的BroadcastProcessFunction ,实现两个方法
    //获取广播状态流
    val stream2: BroadcastStream[MonitorInfo] = streamEnv.addSource(new JdbcReadDataSource[MonitorInfo](classType = classOf[MonitorInfo])).broadcast(GlobalConstants.MONITOR_STATE_DESCRIPTOR) //广播出去

    // connect:内外都可以；join只能内
    stream1.connect(stream2).process(new BroadcastProcessFunction[TrafficInfo,MonitorInfo,OutOfLimitSpeedInfo] {
      override def processElement(value: TrafficInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#ReadOnlyContext, out: Collector[OutOfLimitSpeedInfo]): Unit = {
        val info:MonitorInfo = ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).get(value.monitorId)
        if(info != null){
          var limitSpeed = info.speedLimit
          var realSpeed = value.speed
          if(limitSpeed * 1.1 < realSpeed){
            out.collect(new OutOfLimitSpeedInfo(value.car,value.monitorId,value.roadId,realSpeed,limitSpeed,value.actionTime))
          }
        }
      }

      override def processBroadcastElement(value: MonitorInfo, ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#Context, out: Collector[OutOfLimitSpeedInfo]) = {
        //把广播流中的数据保存到状态中
        ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).put(value.monitorId,value)
      }
    //}).print
    }).addSink(new WriteDataSink[OutOfLimitSpeedInfo](classOf[OutOfLimitSpeedInfo]))
    streamEnv.execute
  }
}
