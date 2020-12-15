package com.opensourceteams.module.bigdata.flink.example.datastream.worldcount.nc

import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * nc -lk 1234  输入数据
 */
object SocketWindowWordCountLocal {

  def main(args: Array[String]): Unit = {

    val port = 1234

    val configuration: Configuration = ConfigurationUtil.getConfiguration(true)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, configuration)

    val dataStream = env.socketTextStream("10.21.20.186", port, '\n')

    import org.apache.flink.streaming.api.scala._

    val textResult = dataStream.flatMap(w => w.split("\\s")).map(w => WordWithCount(w, 1))
      .keyBy("word")
      /**
       * 每20秒刷新一次，相当于重新开始计数，
       * 好处，不需要一直拿所有的数据统计
       * 只需要在指定时间间隔内的增量数据，减少了数据规模
       */
      .timeWindow(Time.seconds(5))
      .sum("count")

    textResult.print().setParallelism(1)


    if (args == null || args.length == 0) {
      env.execute("默认作业")
    } else {
      env.execute(args(0))
    }

    println("结束")

  }


  case class WordWithCount(word: String, count: Long)


}
