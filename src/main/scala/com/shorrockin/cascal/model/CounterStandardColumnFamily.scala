package com.shorrockin.cascal.model
import java.nio.ByteBuffer
import com.shorrockin.cascal.serialization._

case class CounterStandardColumnFamily(val value:String, val keyspace:Keyspace) extends ColumnFamily[CounterStandardKey] {
  def \(value:String) = new CounterStandardKey(StringSerializer.toByteBuffer(value), this)
  def \(value:ByteBuffer) = new CounterStandardKey(value, this)
  
  override def toString = "%s \\ StandardCounterColumnFamily(value = %s)".format(keyspace.toString, value)
}