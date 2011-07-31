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

trait Schema {
  val keyspace = "Test"
  
  val colMetaData = Map[ByteBuffer, ColumnDefinition](
    byteBuffer("column1") -> new ColumnDefinition("column1", BytesType.instance, IndexType.KEYS, null),
    byteBuffer("longColumn") -> new ColumnDefinition("longColumn", LongType.instance, IndexType.KEYS, null))
    
  val standardIndexedCf = cfMetaData("StandardIndexed", ColumnFamilyType.Standard, BytesType.instance)
  standardIndexedCf.columnMetadata(colMetaData)
  
  val ksMetaData = new KSMetaData(keyspace, classOf[SimpleStrategy], Map("replication_factor" -> "1"), false,
      cfMetaData("Standard", ColumnFamilyType.Standard, BytesType.instance),
      cfMetaData("Super", ColumnFamilyType.Super, TimeUUIDType.instance),
      cfMetaData("SuperBytes", ColumnFamilyType.Super, BytesType.instance),
      standardIndexedCf)
  
  def cfMetaData(name: String, cfType: ColumnFamilyType, colType: AbstractType[_]) = {
    new CFMetaData(keyspace, name, cfType, colType, null).keyCacheSize(0);
  }
  
  def loadSchema() = {
    for (cfMetaData <- ksMetaData.cfMetaData().values()) 
      CFMetaData.map(cfMetaData)
        
    DatabaseDescriptor.setTableDefinition(ksMetaData, DatabaseDescriptor.getDefsVersion())
  }
}