package com.shorrockin.cascal

import scala.collection.JavaConversions._
import org.apache.cassandra.config.{CFMetaData, DatabaseDescriptor, KSMetaData}
import org.apache.cassandra.db.marshal.{AbstractType, BytesType, TimeUUIDType}
import org.apache.cassandra.db.ColumnFamilyType
import org.apache.cassandra.locator.SimpleStrategy
import java.nio.ByteBuffer
import org.apache.cassandra.config.ColumnDefinition
import org.apache.cassandra.thrift.IndexType
import com.shorrockin.cascal.utils.Conversions.byteBuffer
import org.apache.cassandra.db.marshal.LongType
import com.shorrockin.cascal.test.Schema

trait CascalSchema extends Schema {
  val keyspace = "Test"
  val replicationFactor = 1
  val timeout = 1100
  val strategyClass = classOf[SimpleStrategy]
  
  val colMetaData = Map[ByteBuffer, ColumnDefinition](
    byteBuffer("column1") -> new ColumnDefinition("column1", BytesType.instance, IndexType.KEYS, null, "column1Indx"),
    byteBuffer("longColumn") -> new ColumnDefinition("longColumn", LongType.instance, IndexType.KEYS, null, "longColumnIndx"))
    
  val standardIndexedCf = cfMetaData("StandardIndexed", ColumnFamilyType.Standard, BytesType.instance)
  standardIndexedCf.columnMetadata(colMetaData)
  
  val cfMetaDatas = Seq(
      cfMetaData("Standard", ColumnFamilyType.Standard, BytesType.instance),
      cfMetaData("Super", ColumnFamilyType.Super, TimeUUIDType.instance),
      cfMetaData("SuperBytes", ColumnFamilyType.Super, BytesType.instance),
      standardIndexedCf)
}