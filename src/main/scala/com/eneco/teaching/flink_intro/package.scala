package com.eneco.teaching

import scala.language.implicitConversions

package object flink_intro {
  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) => props.put(k, v); props }
  }
}