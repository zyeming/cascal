package com.shorrockin.cascal.utils

import java.nio.charset.Charset
import com.shorrockin.cascal.model.{Column, Keyspace}
import java.util.{Date, UUID => JavaUUID}
import com.shorrockin.cascal.serialization._
import java.nio.ByteBuffer

/**
 * some implicits to assist with common conversions
 */
object Conversions {
  val utf8 = Charset.forName("UTF-8")

  implicit def keyspace(str:String) = new Keyspace(str)

  implicit def byteBuffer(date:Date):ByteBuffer = DateSerializer.toByteBuffer(date)
  implicit def date(bytes:ByteBuffer):Date = DateSerializer.fromByteBuffer(bytes)
  implicit def string(date:Date):String = DateSerializer.toString(date)

  implicit def byteBuffer(b:Boolean):ByteBuffer = BooleanSerializer.toByteBuffer(b)
  implicit def boolean(bytes:ByteBuffer):Boolean = BooleanSerializer.fromByteBuffer(bytes)
  implicit def string(b:Boolean):String = BooleanSerializer.toString(b)

  implicit def byteBuffer(b:Float):ByteBuffer = FloatSerializer.toByteBuffer(b)
  implicit def float(bytes:ByteBuffer):Float = FloatSerializer.fromByteBuffer(bytes)
  implicit def string(b:Float):String = FloatSerializer.toString(b)

  implicit def byteBuffer(b:Double):ByteBuffer = DoubleSerializer.toByteBuffer(b)
  implicit def double(bytes:ByteBuffer):Double = DoubleSerializer.fromByteBuffer(bytes)
  implicit def string(b:Double):String = DoubleSerializer.toString(b)

  implicit def byteBuffer(l:Long):ByteBuffer = LongSerializer.toByteBuffer(l)
  implicit def long(bytes:ByteBuffer):Long = LongSerializer.fromByteBuffer(bytes)
  implicit def string(l:Long):String = LongSerializer.toString(l)

  implicit def byteBuffer(i:Int):ByteBuffer = IntSerializer.toByteBuffer(i)
  implicit def int(bytes:ByteBuffer):Int = IntSerializer.fromByteBuffer(bytes)
  implicit def string(i:Int) = IntSerializer.toString(i)

  implicit def byteBuffer(str:String):ByteBuffer = StringSerializer.toByteBuffer(str)
  implicit def string(bytes:ByteBuffer):String = StringSerializer.fromByteBuffer(bytes)

  implicit def string(source:JavaUUID) = UUIDSerializer.toString(source)
  implicit def uuid(source:String) = UUIDSerializer.fromString(source)
  implicit def byteBuffer(source:JavaUUID):ByteBuffer = UUIDSerializer.toByteBuffer(source)

  implicit def string(col:Column[_]):String = {
    "%s -> %s (time: %s)".format(Conversions.string(col.name),
                                 Conversions.string(col.value),
                                 col.time)
  }

  implicit def toSeqBytes(values:Seq[String]) = values.map { (s) => Conversions.byteBuffer(s) }
  implicit def toJavaList[T](l: Seq[T]):java.util.List[T] = l.foldLeft(new java.util.ArrayList[T](l.size)){(al, e) => al.add(e); al}
}
