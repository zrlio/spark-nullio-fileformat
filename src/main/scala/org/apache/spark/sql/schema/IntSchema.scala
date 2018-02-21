package org.apache.spark.sql.schema

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{NullDataSchema, RowGenerator}

import scala.util.Random

/**
  * Created by atr on 27.11.17.
  *
  * This is meant to generate just the ints
  */
object IntSchema extends NullDataSchema with Serializable {

  final override def getSchema: StructType = new StructType().add("intValue", IntegerType)

  final override def numFields: Int = 1

  final override def getFixedSizeBytes: Long = java.lang.Integer.BYTES

  final override def numVariableFields: Long = 0
}

class IntSchema (intRange: Int) extends RowGenerator {
  private val random = new Random(System.nanoTime())
  private val unsafeRow = new UnsafeRow(IntSchema.numFields)
  private val bufferHolder = new BufferHolder(unsafeRow)
  private val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, IntSchema.numFields)

  override def nextRow(): UnsafeRow = {
    bufferHolder.reset()
    unsafeRowWriter.write(0, random.nextInt(intRange))
    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}