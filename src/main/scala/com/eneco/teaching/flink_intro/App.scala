package com.eneco.teaching.flink_intro

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.time.Duration
import java.util.TimeZone
import java.util.concurrent.TimeUnit

//noinspection ScalaDeprecation
object App {
  def main(args: Array[String]): Unit = {

    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Amsterdam"))

    implicit val appConfig: AppConfig = ConfigSource.default.load[AppConfig].right.get

    val flinkConfig = new Configuration()
    flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem")
    flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, appConfig.checkpointPath)
    flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, appConfig.savepointPath)

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig)

    env.setMaxParallelism(10)

    env.getCheckpointConfig.setCheckpointInterval(Duration.ofSeconds(30).toMillis)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(Duration.ofSeconds(2).toMillis)
    env.getCheckpointConfig.setCheckpointTimeout(Duration.ofMinutes(30).toMillis)

    //bounded text sink

    val text: DataStream[String] = env
      .readTextFile(f"${appConfig.sourceLocalPath}test.csv")
      .filter((row: String) => row.startsWith("9"))

    val resultStringSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path(f"${appConfig.resultLocalPath}test_text"),
        new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(2)
          .withInactivityInterval(2)
          .withMaxPartSize(100 * 100 * 100)
          .build())
      .build()

    text.addSink(resultStringSink)

    env.setParallelism(5)

    //kafka unbounded

    val kafkaConsumer: KafkaSource[String] = KafkaSource.builder[String]()
      .setTopics(appConfig.kafkaConf.sourceTopicName)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperties(appConfig.kafkaConf.kafkaCommonParams) // check implicit conversions in package
      .build()

    val watermarkStrategy: WatermarkStrategy[String] = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(appConfig.kafkaConf.maxOutOfOrdernessSec))
      .withTimestampAssigner(new SerializableTimestampAssigner[String] {
        override def extractTimestamp(record: String, recordTimestamp: Long): Long = System.currentTimeMillis
      })
      .withIdleness(Duration.ofSeconds(appConfig.kafkaConf.maxOutOfOrdernessSec))

    val kafkaDataDs: DataStreamSource[String] = env.fromSource(
        kafkaConsumer,
        watermarkStrategy,
        "Kafka Source")
      .setParallelism(6)

    val rawSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path(f"${appConfig.resultLocalPath}kafka"),
        new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.SECONDS.toMillis(15))
          .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()

    kafkaDataDs.addSink(rawSink)

    val kafkaDataTransformedDs: SingleOutputStreamOperator[String] = kafkaDataDs
      .filter((str: String) => str.contains('A'))
      .setParallelism(5)
      .map[String]((str: String) => {
        f"${System.currentTimeMillis() / 10000},$str"
      })

    env.setParallelism(1)

    val transformedSink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(
        new Path(f"${appConfig.resultLocalPath}kafka_transformed"),
        new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.SECONDS.toMillis(15))
          .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
          .withMaxPartSize(1024 * 1024 * 1024)
          .build())
      .build()

    val kafkaDataWindowedDs: SingleOutputStreamOperator[String] = kafkaDataTransformedDs
      .keyBy((str: String) => str.split(",")(0))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new MyProcessWindowFunction())

    kafkaDataWindowedDs
      .addSink(transformedSink)

    env.execute()

  }

}
