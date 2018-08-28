import java.lang
import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

/**
  *
  * <p>
  * 类名称：WordCOunt
  * </p>
  * <p>
  * 类描述：${DESCRIPTION}
  * </p>
  * <p>
  * 创建人：sun
  * </p>
  * <p>
  * 创建时间：2018-08-28 10:09
  * </p>
  * <p>
  * 修改人：
  * </p>
  * <p>
  * 修改时间：
  * </p>
  * <p>
  * 修改备注：
  * </p>
  * <p>
  * Copyright (c) 版权所有
  * </p>
  *
  * @version 1.0.0
  *
  */
object WordCount {

  import scala.collection.JavaConversions._

  def main(args: Array[String]): Unit = {
    val mapper: Mapper[LongWritable, Text, Text, IntWritable] = new Mapper[LongWritable, Text, Text, IntWritable]() {
      private val intWritable = new IntWritable(1)
      override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = value.toString.split(" ").foreach(str => context.write(new Text(str), intWritable))
    }

    val reducer: Reducer[Text, IntWritable, Text, IntWritable] = new Reducer[Text, IntWritable, Text, IntWritable]() {
      override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = context.write(key, new IntWritable(values.iterator().map(_.get()).sum))
    }
    val configuration = new Configuration()
    val job = new Job(configuration)
    job.setMapperClass(mapper.getClass)
    job.setReducerClass(reducer.getClass)
    job.setJarByClass(WordCount.getClass)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path("./input"))
    FileOutputFormat.setOutputPath(job, new Path("./output"))
    val bool = job.waitForCompletion(true)
    println(bool)

  }

}
