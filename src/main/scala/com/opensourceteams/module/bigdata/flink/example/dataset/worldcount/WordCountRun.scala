package com.opensourceteams.module.bigdata.flink.example.dataset.worldcount

import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.api.scala.ExecutionEnvironment


/**
  * 批处理，DataSet WordCount分析
  */
object WordCountRun {


  def main(args: Array[String]): Unit = {

    //调试设置超时问题
    val env : ExecutionEnvironment= ExecutionEnvironment.createLocalEnvironment(ConfigurationUtil.getConfiguration(true))
    env.setParallelism(2)

    val dataSet = env.readTextFile("D:\\workspace\\flink-maven-scala\\sink-data\\csv\\a.csv")


    import org.apache.flink.streaming.api.scala._
    val result = dataSet.flatMap(x => x.split(",")).map((_,1)).groupBy(0).sum(1)



    result.print()




  }

}
