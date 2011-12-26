package com.shorrockin.cascal.model
import java.nio.ByteBuffer
import org.apache.cassandra.thrift.ColumnOrSuperColumn

case class StandardCounterKey(val value:String, val family:StandardCounterColumnFamily)
	extends Key[CounterColumn[StandardCounterKey], Seq[CounterColumn[StandardCounterKey]]]
	with StandardColumnContainer[CounterColumn[StandardCounterKey], Seq[CounterColumn[StandardCounterKey]], Long] {

  def \(name:ByteBuffer) = new CounterColumn(name, None, this)
  def \(name:ByteBuffer, value:Long) = new CounterColumn(name, Some(value), this)

  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[CounterColumn[StandardCounterKey]] = {
    results.map { (result) =>
      val column = result.getCounter_column
      \(ByteBuffer.wrap(column.getName), column.getValue)
    }
  }

  override def toString = "%s \\ StandardCounterKey(value = %s)".format(family.toString, value)
}
