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

package org.apache.spark.sql

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

/**
  * Created by atr on 10.08.17.
  */
class NullOutputWriter(path: String, context: TaskAttemptContext) extends OutputWriter {
  private val init = System.nanoTime()
  private var start = 0L
  private var itemsInternalRow:Long = 0L
  private var itemsRow:Long = 0L

  override def close(): Unit = {
    val tx = System.nanoTime() - start
    val timePerRow = if(this.itemsRow == 0){
      0
    } else {
      tx/this.itemsRow
    }
    val timePerInternalRow = if(this.itemsInternalRow == 0){
      0
    } else {
      tx/this.itemsInternalRow
    }

    val str = "closing Spark NullOutputWriter. " +
        "init-to-write: " + (start - init).toFloat/1000 + " usec " +
        " runTime: " + tx.toFloat/1000 + " usec " +
        " #InternalRow: " + this.itemsInternalRow +
        " #Rows: " + this.itemsRow +
        " time/Internalrow: " + timePerInternalRow + " nsec" +
        " time/Row: " + timePerRow + " nsec"
    /* print the string */
    System.err.println(str)
  }

  override def write(row: InternalRow): Unit = {
    if(start == 0) {
      start = System.nanoTime()
      //new Exception("First Row writer, stack below").printStackTrace()
    }
    this.itemsInternalRow+=1
  }

}