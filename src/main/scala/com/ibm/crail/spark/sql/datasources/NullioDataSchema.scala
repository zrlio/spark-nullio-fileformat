package com.ibm.crail.spark.sql.datasources

import org.apache.spark.sql.types.StructType

/**
  * Created by atr on 10.08.17.
  */
trait NullioDataSchema {
  def getSchema: StructType
  def numFields:Int
}