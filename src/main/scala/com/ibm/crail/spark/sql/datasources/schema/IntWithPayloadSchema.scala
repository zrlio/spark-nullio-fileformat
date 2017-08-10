package com.ibm.crail.spark.sql.datasources.schema

import com.ibm.crail.spark.sql.datasources.NullioDataSchema
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
  * Created by atr on 10.08.17.
  */
case object IntWithPayloadSchema extends NullioDataSchema with Serializable {
  /* series of ints */
  override val numFields = 1

  override def getSchema: StructType = {
    new StructType().add("randInt", IntegerType)
  }
}

class IntWithPayloadSchema {

}
