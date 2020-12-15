package com.opensourceteams.module.bigdata.flink.example.datastream.worldcount.nc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * nc -lk 1234  输入数据
  */
object SocketWindowWordCountRemote {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("standalone.com",6123,"D:\\workspace\\flink-maven-scala\\target\\flink-maven-scala-2-0.0.1.jar")

    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("10.21.20.186", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult = dataStream.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word")
      /**
        * 每5秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      .timeWindow(Time.seconds(5))
      .sum("count" )

    textResult.print().setParallelism(1)

    if(args == null || args.size ==0){
      env.execute("远程提交默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
