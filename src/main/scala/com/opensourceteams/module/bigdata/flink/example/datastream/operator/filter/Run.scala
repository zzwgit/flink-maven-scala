package com.opensourceteams.module.bigdata.flink.example.datastream.operator.filter

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("localhost", port, '\n')

    val dataStreamMap = dataStream.filter( x => (x.contains("a")))

    dataStreamMap.print()


    if(args == null || args.size ==0){
      env.execute("默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }



}
