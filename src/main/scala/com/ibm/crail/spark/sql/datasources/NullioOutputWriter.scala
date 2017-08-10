package com.ibm.crail.spark.sql.datasources

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

/**
  * Created by atr on 10.08.17.
  */
class NullioOutputWriter (path: String, context: TaskAttemptContext) extends OutputWriter {
  private val init = System.nanoTime()
  private var start = 0L
  private var itemsInternalRow:Long = 0L

  override def close(): Unit = {
    val end = System.nanoTime()
    val str = if(this.itemsInternalRow == 0) {
      /* if zero then we can just calculate the init time, all other entries are zero */
      start = System.nanoTime()
      "closing Atr(Null)OutputWriter. " +
        "initPause: " + (start - init).toFloat/1000 + " usec " +
        " runTime: " + 0 + " usec " +
        " #InternalRow: " + this.itemsInternalRow +
        " time/row: " + 0 + " nsec"
    } else {
      /* sensible calculation of numbers when we have processed more then  1 entries */
      val tx = end - start
      "closing Atr(Null)OutputWriter. " +
        "initPause: " + (start - init).toFloat/1000 + " usec " +
        " runTime: " + tx.toFloat/1000 + " usec " +
        " #InternalRow: " + this.itemsInternalRow +
        " time/row: " + tx/this.itemsInternalRow + " nsec"
    }
    /* print the string */
    System.err.println(str)
  }

  override def write(row: Row): Unit = {
    if(this.itemsInternalRow == 0) {
      start = System.nanoTime()
      //new Exception("First Row writer, stack below").printStackTrace()
    }
    this.itemsInternalRow+=1
  }

  override def writeInternal(row: InternalRow): Unit = {}
}