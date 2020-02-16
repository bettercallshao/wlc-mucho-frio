package wlc

import math._
import collection.mutable.PriorityQueue
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Def {
  //             ts    ip      url
  type LogRow = (Long, String, String)
  //             ip      start len   cnt
  type SesRow = (String, Long, Long, Long)
  //             const ip      len
  type EngRow = (Long, String, Long)
  // Watermark lag (ms)
  val watermarkLag = 3000L
  // Gap limit for session breaking
  val minimumGapLength = 15L * 60 * 1000
  // Time window size
  val windowSize = Time.hours(120)
  // Minimum session length
  val minimumSessionLength = 1000L
  // Top K most engaged IPs
  val topK = 10
}

// Parse log and assign timestamp and watermark
class LogParser extends AssignerWithPeriodicWatermarks[Def.LogRow] {

  // Keep timestamp for periodic watermarking
  var currentTimestamp: Long = 0

  override def extractTimestamp(
      element: Def.LogRow,
      previousElementTimestamp: Long
  ): Long = {
    val timestamp = element._1
    currentTimestamp = max(currentTimestamp, timestamp)
    timestamp
  }

  override def getCurrentWatermark(): Watermark = {
    new Watermark(currentTimestamp - Def.watermarkLag)
  }
}

// Create sessions and transform
class SessionMapper extends RichMapFunction[Def.LogRow, Def.SesRow] {

  // Flink managed state
  private var state: ValueState[(Long, Long, Set[String])] = _

  override def map(input: Def.LogRow): Def.SesRow = {
    // Get state and initialize
    val remote = state.value()
    val currentState = if (remote == null) {
      (Long.MinValue, Long.MinValue, Set[String]())
    } else {
      remote
    }

    // Call utility method
    val (newState, output) =
      Mapper.map(currentState, input, Def.minimumGapLength)

    // Update state with new values
    state.update(newState)

    // Output
    output
  }

  override def open(parameters: Configuration): Unit = {
    // Declare state
    state = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long, Set[String])](
        "state",
        createTypeInformation[(Long, Long, Set[String])]
      )
    )
  }
}

// Top K aggregator
class TopKFinder
    extends WindowFunction[Def.EngRow, Def.EngRow, Tuple, TimeWindow] {
  override def apply(
      key: Tuple,
      window: TimeWindow,
      iterator: Iterable[Def.EngRow],
      collector: Collector[Def.EngRow]
  ): Unit = {
    // Keep the top K running maximums
    val heap = new PriorityQueue[(Long, String)]()
    iterator.foreach { input =>
      heap += ((-input._3, input._2))
      if (heap.size > Def.topK) {
        heap.dequeue()
      }
    }
    // Add top K to out collection in right order
    heap.dequeueAll.reverse.foreach { item =>
      collector.collect((1L, item._2, -item._1))
    }
  }
}

// Main class for flink application
object Wlc {

  def main(args: Array[String]): Unit = {

    // Define environment
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val fileInput = params.getRequired("input")
    val sessionOutput = params.getRequired("session")
    val averageOutput = params.getRequired("average")
    val engagedOutput = params.getRequired("engaged")

    // Prepare input
    val format = new TextInputFormat(new Path(fileInput))
    val source = env.readFile(format, fileInput)

    // Parse log and add timestamp
    val parsed = source
      .map(Parser.parse(_))
      .assignTimestampsAndWatermarks(new LogParser())

    // Sessionize and calculate stat
    val sessioned = parsed
    // Key by IP to assign sessions
      .keyBy(1)
      .map(new SessionMapper())
      // Key by IP and session id (start timestamp) to reduce stat
      .keyBy(0, 1)
      // A long enough window is necessary to allow reduce
      .timeWindow(Def.windowSize)
      .reduce((a, b) => (a._1, a._2, max(a._3, b._3), a._4 + b._4))
      // Only consider sessions longer than 1 second
      .filter(ses => ses._3 > Def.minimumSessionLength)

    // Session output
    sessioned.writeAsCsv(sessionOutput, WriteMode.OVERWRITE)

    // Average session length
    sessioned
    // Only keep a constant and session length
      .map(ses => (1L, ses._3))
      // Key by a constant to create a keyed stream
      // TODO: this is unnecessary
      .keyBy(0)
      .timeWindow(Def.windowSize)
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      // Calculate average from sum and count
      .map(pair => Tuple1(pair._2 / pair._1))
      .writeAsCsv(averageOutput, WriteMode.OVERWRITE)

    // IPs with the most cumulative session length
    sessioned
    // Only keep a constant, IP and session length
      .map(ses => (1L, ses._1, ses._3))
      // Key by IP
      .keyBy(1)
      .timeWindow(Def.windowSize)
      // Take the sum of all session length
      .reduce((a, b) => (1L, a._2, a._3 + b._3))
      // Key by constant to create keyed stream
      .keyBy(0)
      .timeWindow(Def.windowSize)
      .apply(new TopKFinder())
      .writeAsCsv(engagedOutput, WriteMode.OVERWRITE)

    env.execute()
  }
}
