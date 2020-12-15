package com.opensourceteams.module.bigdata.flink.example.datastream.worldcount.nc

import java.time.ZoneId

import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
 * nc -lk 1234  输入数据
 */
object SocketWindowWordCountLocalSinkHDFS {


  def main(args: Array[String]): Unit = {

    val port = 1234

    val configuration: Configuration = ConfigurationUtil.getConfiguration(true)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, configuration)

    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("10.21.20.186", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val dataStreamDeal = dataStream.flatMap(w => w.split("\\s")).map(w => WordWithCount(w, 1))
      .keyBy("word")
      /**
       * 每20秒刷新一次，相当于重新开始计数，
       * 好处，不需要一直拿所有的数据统计
       * 只需要在指定时间间隔内的增量数据，减少了数据规模
       */
      .timeWindow(Time.seconds(30))
      .sum("count")

    val bucketingSink = new BucketingSink[WordWithCount]("file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/sink-data")

    bucketingSink.setBucketer(new DateTimeBucketer[WordWithCount]("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")))
    bucketingSink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    bucketingSink.setBatchRolloverInterval(2 * 1000); // this is 20 mins
    bucketingSink.setInactiveBucketCheckInterval(2 * 1000)
    bucketingSink.setInactiveBucketThreshold(2 * 1000)
    dataStreamDeal.setParallelism(1)
      .addSink(bucketingSink)


    if (args == null || args.size == 0) {
      env.execute("默认作业")
    } else {
      env.execute(args(0))
    }

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
