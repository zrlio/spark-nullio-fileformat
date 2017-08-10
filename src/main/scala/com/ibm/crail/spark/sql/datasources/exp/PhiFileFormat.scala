/*
package com.ibm.crail.spark.sql.datasources.exp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Random

/**
  * Created by atr on 25.04.17.
  */

class PhiOutputWriter (path: String, context: TaskAttemptContext) extends OutputWriter {
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
}

trait PhiDataSchema {
  def getSchema: StructType
  def numFields:Int
}

class PhiDataSchemaParquetExample extends PhiDataSchema with Serializable {
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


class PhiDataSchemaInt extends PhiDataSchema with Serializable {
  /* series of ints */
  override val numFields = 1

  override def getSchema: StructType = {
    new StructType().add("randInt", IntegerType)
  }
}

object PhiFileFormat {
  val KEY_INPUT_ROWS = "inputrows"
  val KEY_PAYLOAD_SIZE = "payloadsize"
  val KEY_INT_RANGE = "intrange"
  val KEY_SCHEMA = "schema"
  val KEY_PATH = "path"
  val validKeys = Set(KEY_INPUT_ROWS,
    KEY_PAYLOAD_SIZE,
    KEY_INT_RANGE,
    KEY_SCHEMA,
    KEY_PATH)
}

class PhiFileFormat extends FileFormat with DataSourceRegister with Serializable {

  /* yes, it is splitable */
  val splitable = SparkEnv.get.conf.getBoolean("spark.sql.input.AtrFileFormat.splitable", true)

  override def shortName(): String = "phi"

  override def toString: String = "phi"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        System.err.println("allocating a new PhiOutputWriter to file: " + path)
        new PhiOutputWriter(path, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".phi"
      }
    }
  }

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = {
    splitable
  }

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {

    val schString = options.getOrElse("schema", "ParquetExample")
    val schema:PhiDataSchema = if(schString.compareToIgnoreCase("ParquetExample") == 0) {
      new PhiDataSchemaParquetExample
    } else {
      new PhiDataSchemaInt
    }
    System.err.println("called infer schema, returning " + schema.getSchema)
    Some(schema.getSchema)
  }

  override def buildReader( sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    System.err.println(" ----- Null Source/Sink options (all lower caps) ----- ")
    for (o <- options){
      System.err.println(" " + o._1 + " : " + o._2)
      if(!PhiFileFormat.validKeys.contains(o._1.toLowerCase)){
        throw new Exception("Illegal option : " + o._1 +
          " , valid options are: " + PhiFileFormat.validKeys)
      }
    }
    System.err.println(" ------------------------------------ ")
    /* we first must parse out params from the options */
    val inputRows = options.getOrElse(PhiFileFormat.KEY_INPUT_ROWS,
      "1000000").toLong
    val sch = options.getOrElse(PhiFileFormat.KEY_SCHEMA, "ParquetExample")
    val payloadSize = options.getOrElse(PhiFileFormat.KEY_PAYLOAD_SIZE,
      "1024").toInt
    val intRange = options.getOrElse(PhiFileFormat.KEY_INT_RANGE,
      Integer.MAX_VALUE.toString).toInt

    System.err.println("###########################################################")
    System.err.println("AtrFileReader: schema " + requiredSchema +
      " inputRows: " + inputRows +
      " intRange: " + intRange +
      " payloadSize: " + payloadSize)
    System.err.println("###########################################################")

    val schema:PhiDataSchema = if(sch.compareToIgnoreCase("ParquetExample") == 0) {
      new PhiDataSchemaParquetExample
    } else {
      new PhiDataSchemaInt
    }

    (file: PartitionedFile) => {
      val generator = if(schema.isInstanceOf[PhiDataSchemaParquetExample]){
          new PhiParquetExampleRowGenerator(schema, payloadSize, intRange)
        } else {
          throw new Exception("Not implemented yet")
        }
      new AtrIterator(generator, inputRows)
    }
  }
}

trait RowGenerator {
  def nextRow():UnsafeRow
}

class PhiParquetExampleRowGenerator(schema: PhiDataSchema, maxSize: Int, intRange: Int) extends RowGenerator{
  val random = new Random(System.nanoTime())
  val unsafeRow = new UnsafeRow(schema.numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, schema.numFields)

  val byteArray = new Array[Byte](maxSize)
  val charArray = random.alphanumeric.take(maxSize).mkString.toCharArray
  for (i <- 0 until maxSize){
    byteArray(i) = charArray(i).toByte
  }
  val utf8String:UTF8String = UTF8String.fromBytes(byteArray)

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

/* this should always take just the int iterator */
class AtrIterator(rowGenerator: RowGenerator, itemsToGenerate: Long) extends Iterator[InternalRow] {
  private var soFar = 0L
  override def hasNext: Boolean = soFar < itemsToGenerate

  override def next(): InternalRow = {
    //    if(soFar == 0L){
    //      /* first time it is called */
    //      new Exception("Called next() on AtrIterator on the reader").printStackTrace()
    //    }
    /* return the Row that the generator generates */
    soFar+=1
    rowGenerator.nextRow()
  }
}*/
