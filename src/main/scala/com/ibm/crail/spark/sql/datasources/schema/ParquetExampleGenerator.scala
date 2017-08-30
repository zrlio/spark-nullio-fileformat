/*
 * spark-nullio file format
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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

  // same in the parquet-generator
  // https://github.com/zrlio/parquet-generator/blob/master/src/main/scala/com/ibm/crail/spark/tools/schema/ParquetExample.scala
  // case class ParquetExample(intKey:Int, randLong: Long, randDouble: Double, randFloat: Float, randString: String)
  override def getSchema: StructType = {
    new StructType().add("intKey", IntegerType)
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