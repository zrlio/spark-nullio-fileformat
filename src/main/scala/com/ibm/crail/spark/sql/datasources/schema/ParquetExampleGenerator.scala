package com.ibm.crail.spark.sql.datasources.schema

import com.ibm.crail.spark.sql.datasources.{NullioDataSchema, RowGenerator}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Random

/**
  * Created by atr on 10.08.17.
  */

case object ParquetExampleSchema extends NullioDataSchema with Serializable {
  /* from the parquet generator tool, we have this schema that we want to generate  */
  override val numFields = 5

  override def getSchema: StructType = {
    new StructType().add("randInt", IntegerType)
      .add("randLong", LongType)
      .add("randDouble", DoubleType)
      .add("randFloat", FloatType)
      .add("randString", StringType)
  }
}

class ParquetExampleGenerator(maxSize: Int, intRange: Int) extends RowGenerator {
  private val random = new Random(System.nanoTime())
  private val unsafeRow = new UnsafeRow(ParquetExampleSchema.numFields)
  private val bufferHolder = new BufferHolder(unsafeRow)
  private val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, ParquetExampleSchema.numFields)

  private val byteArray = new Array[Byte](maxSize)
  private val charArray = random.alphanumeric.take(maxSize).mkString.toCharArray
  for (i <- 0 until maxSize){
    byteArray(i) = charArray(i).toByte
  }
  private val utf8String:UTF8String = UTF8String.fromBytes(byteArray)

  def nextRow():UnsafeRow = {
    /* we write it once here */
    bufferHolder.reset()
    /* we are here anyways why not reset the key */
    unsafeRowWriter.write(0, random.nextInt(intRange))
    unsafeRowWriter.write(1, 42L)
    unsafeRowWriter.write(2, 0.42)
    unsafeRowWriter.write(3, 0.42f)
    /* flip a byte in the array somewhere */
    byteArray(random.nextInt(maxSize)) = random.nextPrintableChar().toByte
    /* copy it out again - utf8 has a reference to the array */
    unsafeRowWriter.write(4, utf8String)
    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}