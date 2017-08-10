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

package com.ibm.crail.spark.sql.datasources

import com.ibm.crail.spark.sql.datasources.schema.{IntWithPayloadSchema, ParquetExampleGenerator, ParquetExampleSchema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

object NullioFileFormat {
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

class NullioFileFormat extends FileFormat with DataSourceRegister with Serializable {

  private var schema:NullioDataSchema = _
  /* yes, it is splitable */
  private val splitable = SparkEnv.get.conf.getBoolean("spark.sql.input.NullioFileFormat.splitable", true)

  override def shortName(): String = "nullio"

  override def toString: String = "nullio"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        System.err.println("allocating a new NullioOutputWriter to file: " + path)
        new NullioOutputWriter(path, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".nullio"
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
    schema = if(schString.compareToIgnoreCase("ParquetExample") == 0) {
      ParquetExampleSchema
    } else {
      IntWithPayloadSchema
    }
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
      if(!NullioFileFormat.validKeys.contains(o._1.toLowerCase)){
        throw new Exception("Illegal option : " + o._1 +
          " , valid options are: " + NullioFileFormat.validKeys)
      }
    }
    System.err.println(" ------------------------------------ ")
    /* we first must parse out params from the options */
    val inputRows = options.getOrElse(NullioFileFormat.KEY_INPUT_ROWS,
      "1000").toLong
    val sch = options.getOrElse(NullioFileFormat.KEY_SCHEMA, "ParquetExample")
    val payloadSize = options.getOrElse(NullioFileFormat.KEY_PAYLOAD_SIZE,
      "32").toInt
    val intRange = options.getOrElse(NullioFileFormat.KEY_INT_RANGE,
      Integer.MAX_VALUE.toString).toInt

    System.err.println("###########################################################")
    System.err.println("NullioFileReader: schema " + requiredSchema +
      " inputRows: " + inputRows +
      " intRange: " + intRange +
      " payloadSize: " + payloadSize)
    System.err.println("###########################################################")

    (file: PartitionedFile) => {
      /* This is super important - don't move this match outside the function otherwise generators
      become part of the clousre and they are not serializable. You will get something like:
      Serialization stack:
      - object not serializable (class: com.ibm.crail.spark.sql.datasources.schema.ParquetExampleGenerator, value: com.ibm.crail.spark.sql.datasources.schema.ParquetExampleGenerator@56929c5b)
      - field (class: com.ibm.crail.spark.sql.datasources.NullioFileFormat$$anonfun$buildReader$2, name: generator$1, type: class com.ibm.crail.spark.sql.datasources.schema.ParquetExampleGenerator)
      - object (class com.ibm.crail.spark.sql.datasources.NullioFileFormat$$anonfun$buildReader$2, <function1>)
      - field (class: org.apache.spark.sql.execution.datasources.FileFormat$$anon$1, name: dataReader$1, type: interface scala.Function1)
      - object (class org.apache.spark.sql.execution.datasources.FileFormat$$anon$1, <function1>)
      - field (class: org.apache.spark.sql.execution.datasources.FileScanRDD, name: org$apache$spark$sql$execution$datasources$FileScanRDD$$readFunction, type: interface scala.Function1)
      - object (class org.apache.spark.sql.execution.datasources.FileScanRDD, FileScanRDD[0] at save at SQLTest.scala:40)
      - field (class: org.apache.spark.NarrowDependency, name: _rdd, type: class org.apache.spark.rdd.RDD)
      - object (class org.apache.spark.OneToOneDependency, org.apache.spark.OneToOneDependency@31a3d7cc)
      - writeObject data (class: scala.collection.immutable.List$SerializationProxy)
      - object (class scala.collection.immutable.List$SerializationProxy, scala.collection.immutable.List$SerializationProxy@432bcc63)
      - writeReplace data (class: scala.collection.immutable.List$SerializationProxy)
      - object (class scala.collection.immutable.$colon$colon, List(org.apache.spark.OneToOneDependency@31a3d7cc))
      - field (class: org.apache.spark.rdd.RDD, name: org$apache$spark$rdd$RDD$$dependencies_, type: interface scala.collection.Seq)
      - object (class org.apache.spark.rdd.MapPartitionsRDD, MapPartitionsRDD[1] at save at SQLTest.scala:40)
      - field (class: scala.Tuple2, name: _1, type: class java.lang.Object)
      - object (class scala.Tuple2, (MapPartitionsRDD[1] at save at SQLTest.scala:40,<function2>))

      I cannot serialize the generators as they contain BufferHolder, UnsafeRowWriter which are not serializable. May
      be someone else have more sensible solution.
     */
      val generator = schema match {
        case ParquetExampleSchema => new ParquetExampleGenerator(payloadSize, intRange)
        case _ => throw new Exception("Not implemented yet")
      }
      new Iterator[InternalRow] {
        private var soFar = 0L
        override def hasNext: Boolean = soFar < inputRows

        override def next(): InternalRow = {
          soFar+=1
          generator.nextRow()
        }
      }
    }
  }
}