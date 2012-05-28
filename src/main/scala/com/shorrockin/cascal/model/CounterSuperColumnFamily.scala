package com.shorrockin.cascal.model
import java.nio.ByteBuffer
import com.shorrockin.cascal.serialization._

class CounterSuperColumnFamily(val value:String, val keyspace:Keyspace) extends ColumnFamily[CounterSuperKey] {
  def \(value:String) = new CounterSuperKey(StringSerializer.toByteBuffer(value), this)
  def \(value:ByteBuffer) = new CounterSuperKey(value, this)

  override def toString = "%s \\\\ CounterSuperColumnFamily(value = %s)".format(keyspace.toString, value)
}