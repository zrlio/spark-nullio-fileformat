package org.apache.spark.sql.schema

import java.util.Random

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.{NullDataSchema, RowGenerator}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StructType}

/**
  * Created by atr on 26.10.17.
  */

case object StoreSalesSchema extends NullDataSchema with Serializable {
  /* from the parquet generator tool, we have this schema that we want to generate  */
  override val numFields = 23

  def getFixedSizeBytes:Long = (10 * java.lang.Integer.BYTES) + (java.lang.Long.BYTES) + (java.lang.Double.BYTES * 12)
  def numVariableFields:Long = 0L

  //
  //  Table("store_sales",
  //    partitionColumns = "ss_sold_date_sk" :: Nil,
  //    'ss_sold_date_sk      .int,
  //    'ss_sold_time_sk      .int,
  //    'ss_item_sk           .int,
  //    'ss_customer_sk       .int,
  //    'ss_cdemo_sk          .int,
  //    'ss_hdemo_sk          .int,
  //    'ss_addr_sk           .int,
  //    'ss_store_sk          .int,
  //    'ss_promo_sk          .int,
  //    'ss_ticket_number     .long,
  //    'ss_quantity          .int,
  //    'ss_wholesale_cost    .decimal(7,2),
  //    'ss_list_price        .decimal(7,2),
  //    'ss_sales_price       .decimal(7,2),
  //    'ss_ext_discount_amt  .decimal(7,2),
  //    'ss_ext_sales_price   .decimal(7,2),
  //    'ss_ext_wholesale_cost.decimal(7,2),
  //    'ss_ext_list_price    .decimal(7,2),
  //    'ss_ext_tax           .decimal(7,2),
  //    'ss_coupon_amt        .decimal(7,2),
  //    'ss_net_paid          .decimal(7,2),
  //    'ss_net_paid_inc_tax  .decimal(7,2),
  //    'ss_net_profit        .decimal(7,2))
  //
  // same in the parquet-generator
  // https://github.com/zrlio/parquet-generator/blob/master/src/main/scala/com/ibm/crail/spark/tools/schema/ParquetExample.scala
  // case class ParquetExample(intKey:Int, randLong: Long, randDouble: Double, randFloat: Float, randString: String)
  override def getSchema: StructType = {
    new StructType().add("ss_sold_date_sk", IntegerType)
      .add("ss_sold_time_sk", IntegerType)
      .add("ss_item_sk", IntegerType)
      .add("ss_customer_sk", IntegerType)
      .add("ss_cdemo_sk", IntegerType)
      .add("ss_hdemo_sk", IntegerType)
      .add("ss_addr_sk", IntegerType)
      .add("ss_store_sk", IntegerType)
      .add("ss_promo_sk", IntegerType)
      .add("ss_ticket_number", LongType)
      .add("ss_quantity", IntegerType)
      .add(" ss_wholesale_cost ", DecimalType(7, 2))
      .add(" ss_list_price ", DecimalType(7, 2))
      .add(" ss_sales_price ", DecimalType(7, 2))
      .add(" ss_ext_discount_amt ", DecimalType(7, 2))
      .add(" ss_ext_sales_price ", DecimalType(7, 2))
      .add(" ss_ext_wholesale_cost.decimal(7,2), ", DecimalType(7, 2))
      .add(" ss_ext_list_price ", DecimalType(7, 2))
      .add(" ss_ext_tax ", DecimalType(7, 2))
      .add(" ss_coupon_amt ", DecimalType(7, 2))
      .add(" ss_net_paid ", DecimalType(7, 2))
      .add(" ss_net_paid_inc_tax ", DecimalType(7, 2))
      .add(" ss_net_profit ", DecimalType(7, 2))
  }
  // DecimalType(7, 2) gives defaultSize as 8
}

class StoreSalesGenerator() extends RowGenerator{
  private val random = new Random(System.nanoTime())
  private val unsafeRow = new UnsafeRow(StoreSalesSchema.numFields)
  private val bufferHolder = new BufferHolder(unsafeRow)
  private val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, StoreSalesSchema.numFields)

  def nextRow():UnsafeRow = {
    /* we write it once here */
    bufferHolder.reset()
    unsafeRowWriter.write(0, 42)
    unsafeRowWriter.write(1, 42)
    unsafeRowWriter.write(2, 42)
    unsafeRowWriter.write(3, 42)
    unsafeRowWriter.write(4, 42)
    unsafeRowWriter.write(5, 42)
    unsafeRowWriter.write(6, 42)
    unsafeRowWriter.write(7, 42)
    unsafeRowWriter.write(8, 42)
    // +9 ints
    unsafeRowWriter.write(9, 42L)
    // +9 ints + 1 long
    unsafeRowWriter.write(10, 42)
    // +9 ints + 1 long + 1 int
    unsafeRowWriter.write(11, 3.14d)
    unsafeRowWriter.write(12, 3.14d)
    unsafeRowWriter.write(13, 3.14d)
    unsafeRowWriter.write(14, 3.14d)
    unsafeRowWriter.write(15, 3.14d)
    unsafeRowWriter.write(16, 3.14d)
    unsafeRowWriter.write(17, 3.14d)
    unsafeRowWriter.write(18, 3.14d)
    unsafeRowWriter.write(19, 3.14d)
    unsafeRowWriter.write(20, 3.14d)
    unsafeRowWriter.write(21, 3.14d)
    unsafeRowWriter.write(22, 3.14d)
    // +9 ints + 1 long + 1 int + 12 double = 23 fields

    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}
