package org.apache.spark.sql

import org.apache.spark.sql.schema.{IntSchema, IntWithPayloadSchema, ParquetExampleSchema, StoreSalesSchema}

import scala.collection.mutable

/**
  * Created by atr on 27.11.17.
  */
object SchemaRegistry {
  val stringToObject = new mutable.HashMap[String, NullDataSchema]()
  /* here we make registration */
  stringToObject.put("ParquetExample".toLowerCase, ParquetExampleSchema)
  stringToObject.put("IntWithPayload".toLowerCase, IntWithPayloadSchema)
  stringToObject.put("Intonly".toLowerCase, IntSchema)
  stringToObject.put("StoreSales".toLowerCase, StoreSalesSchema)

  def getSchema(str:String):Option[NullDataSchema] = {
    stringToObject.get(str.toLowerCase)
  }
}
