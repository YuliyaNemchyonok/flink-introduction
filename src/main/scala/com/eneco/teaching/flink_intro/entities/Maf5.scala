package com.eneco.teaching.flink_intro.entities

case class Maf5Entity(subscriber_no: String,
                      event_time: Long,
                      message_switch_id: String,
                      call_action_code: Option[Int],
                      at_call_dur_sec: Option[Int],
                      call_to_tn_sgsn: String,
                      calling_no_ggsn: String,
                      basic_service_type: String,
                      date: String)
