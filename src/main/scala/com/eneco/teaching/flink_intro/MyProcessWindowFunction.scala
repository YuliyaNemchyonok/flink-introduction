package com.eneco.teaching.flink_intro

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

class MyProcessWindowFunction extends ProcessWindowFunction[String, String, String, TimeWindow] {
  override def process(
                        key: String,
                        context: ProcessWindowFunction[String, String, String, TimeWindow]#Context,
                        input: lang.Iterable[String],
                        out: Collector[String]): Unit = {
    var acc = context.window().toString

    input.forEach(str => acc = acc + "," + str.charAt(0))

    out.collect(acc)
  }
}

