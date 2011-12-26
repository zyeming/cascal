package com.shorrockin.cascal.model

case class StandardCounterColumnFamily(val value:String, val keyspace:Keyspace) extends ColumnFamily[StandardCounterKey] {
  def \(value:String) = new StandardCounterKey(value, this)
  
  override def toString = "%s \\ StandardCounterColumnFamily(value = %s)".format(keyspace.toString, value)
}