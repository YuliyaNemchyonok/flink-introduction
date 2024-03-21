package com.eneco.teaching.flink_intro

import scala.language.implicitConversions

case class KafkaConfig(
                        kafkaCommonParams: Map[String, String],
                        sourceTopicName: String,
                        maxOutOfOrdernessSec: Int,
                        targetTopicName: String
                    )

case class AppConfig(
                        version: String,
                        checkpointPath: String,
                        savepointPath: String,
                        kafkaConf: KafkaConfig,
                        sourceLocalPath: String,
                        resultLocalPath: String
                      )


