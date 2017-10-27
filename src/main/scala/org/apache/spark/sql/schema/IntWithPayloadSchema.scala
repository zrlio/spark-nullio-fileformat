/*
 * spark null file format
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

package org.apache.spark.sql.schema

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructType}
import org.apache.spark.sql.{NullDataSchema, RowGenerator}

import scala.util.Random

/**
  * Created by atr on 10.08.17.
  */
case object IntWithPayloadSchema extends NullDataSchema with Serializable {
  /* series of ints */
  override val numFields = 2

  def getFixedSizeBytes:Long = Integer.BYTES // a single int
  def numVariableFields:Long = 1L // a single variable field

  // same in the parquet-generator
  // https://github.com/zrlio/parquet-generator/blob/master/src/main/scala/com/ibm/crail/spark/tools/schema/IntWithPayload.scala
  // case class IntWithPayload(intKey: Int, payload: Array[Byte])
  override def getSchema: StructType = {
    new StructType().add("intKey", IntegerType).
      add("payload", BinaryType)
  }
}

class IntWithPayloadSchema (maxSize: Int, intRange: Int) extends RowGenerator {

  private val random = new Random(System.nanoTime())
  private val unsafeRow = new UnsafeRow(ParquetExampleSchema.numFields)
  private val bufferHolder = new BufferHolder(unsafeRow)
  private val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, ParquetExampleSchema.numFields)

  private val byteArray = new Array[Byte](maxSize)
  random.nextBytes(byteArray)

  override def nextRow(): UnsafeRow = {
    /* we write it once here */
    bufferHolder.reset()
    /* we are here anyways why not reset the key */
    unsafeRowWriter.write(0, random.nextInt(intRange))
    /* flip a byte in the array somewhere */
    byteArray(random.nextInt(maxSize)) = random.nextInt().toByte
    unsafeRowWriter.write(1, byteArray)
    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}
