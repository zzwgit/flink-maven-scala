package com.opensourceteams.module.bigdata.flink.example.datastream.worldcount.nc

import java.time.ZoneId

import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * nc -lk 1234  输入数据
  */
object SocketWindowWordCountLocalSinkHDFSAndWindowAllAndSorted {

  def main(args: Array[String]): Unit = {

    val port = 1234

    val configuration : Configuration = ConfigurationUtil.getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1,configuration)

    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("10.21.20.186", port, '\n')

    import org.apache.flink.streaming.api.scala._
    val dataStreamDeal = dataStream.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word")
      //将当前window中所有的行记录，发送过来ProcessAllWindowFunction函数中去处理(可以排序，可以对相同key进行处理)
      //缺点，window中数据量大时，就容易内存溢出
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))

      .process(new ProcessAllWindowFunction[WordWithCount,WordWithCount,TimeWindow] {
        override def process(context: Context, elements: Iterable[WordWithCount], out: Collector[WordWithCount]): Unit = {
          val set = new mutable.HashSet[WordWithCount]{}
          for(wordCount <- elements){
            if(set.contains(wordCount)){
              set.remove(wordCount)
              set.add(new WordWithCount(wordCount.word,wordCount.count + 1))
            }else{
              set.add(wordCount)
            }
          }
          val sortSet = set.toList.sortWith( (a,b) => a.word.compareTo(b.word)  < 0 )
          for(wordCount <- sortSet)  out.collect(wordCount)
        }
      })

    dataStreamDeal.print().setParallelism(1)

//    val bucketingSink = new BucketingSink[WordWithCount]("file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/sink-data")
//    bucketingSink.setBucketer(new DateTimeBucketer[WordWithCount]("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")))
//    bucketingSink.setBatchSize(1024 * 1024 * 400 ) // this is 400 MB,
//    bucketingSink.setBatchRolloverInterval( 2 * 1000); // this is 20 mins
//    bucketingSink.setInactiveBucketCheckInterval(2 * 1000)
//    bucketingSink.setInactiveBucketThreshold(2 * 1000)
//    bucketingSink.setAsyncTimeout(1 * 1000)
//    dataStreamDeal.setParallelism(1)
//      .addSink(bucketingSink)

    if(args == null || args.size ==0){
      env.execute("默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}


