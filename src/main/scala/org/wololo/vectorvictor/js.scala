package org.wololo.vectorvictor
import scala.collection.mutable.ArrayBuffer

object js {
  def toJS(arrays : IndexedSeq[ArrayBuffer[Int]]) : String = {
    val arrayStrings = arrays.map { a => s"[${a.mkString(",")}]" }
    s"[${arrayStrings.mkString(",")}"
  }
}