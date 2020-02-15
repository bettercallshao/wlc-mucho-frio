package wlc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.core.fs.Path
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  */
object Wlc {

  def main(args: Array[String]): Unit = {

    // Define environment
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val fileOutput = params.has("output")
    val fileInput = params.get("input")
  
    // Prepare input
    val format = new TextInputFormat(new Path(fileInput))
    val source = env.readFile(format, fileInput)
    val parsed: DataStream[(String, Long, String)] = source
      .map(line => ("a", 1, "b"))

    val aggregated: DataStream[(String, Long, String)] = parsed
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
      .sum(1)

    aggregated.print()

    env.execute()
  }

}
