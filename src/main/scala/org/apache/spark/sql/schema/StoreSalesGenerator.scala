package org.apache.spark.sql.schema

import java.nio.ByteBuffer
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

class StoreSalesGenerator(bufferSize:Int) extends RowGenerator{
  private val random = new Random(System.nanoTime())
  private val unsafeRow = new UnsafeRow(StoreSalesSchema.numFields)
  private val bufferHolder = new BufferHolder(unsafeRow)
  private val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, StoreSalesSchema.numFields)
  private val bb = new Array[Byte](bufferSize)
  random.nextBytes(bb)
  private val bbx = ByteBuffer.wrap(bb)


  def nextRow():UnsafeRow = {
    /* we write it once here */
    bufferHolder.reset()
    var idx = random.nextInt(bufferSize - 8)
    val intV = bbx.getInt(idx)
    val longV = bbx.getLong(idx)
    val doubleV = bbx.getDouble(idx)

    unsafeRowWriter.write(0, intV)
    unsafeRowWriter.write(1, intV)
    unsafeRowWriter.write(2, intV)
    unsafeRowWriter.write(3, intV)
    unsafeRowWriter.write(4, intV)
    unsafeRowWriter.write(5, intV)
    unsafeRowWriter.write(6, intV)
    unsafeRowWriter.write(7, intV)
    unsafeRowWriter.write(8, intV)
    // +9 ints
    unsafeRowWriter.write(9, longV)
    // +9 ints + 1 long
    unsafeRowWriter.write(10, intV)
    // +9 ints + 1 long + 1 int
    unsafeRowWriter.write(11, doubleV)
    unsafeRowWriter.write(12, doubleV)
    unsafeRowWriter.write(13, doubleV)
    unsafeRowWriter.write(14, doubleV)
    unsafeRowWriter.write(15, doubleV)
    unsafeRowWriter.write(16, doubleV)
    unsafeRowWriter.write(17, doubleV)
    unsafeRowWriter.write(18, doubleV)
    unsafeRowWriter.write(19, doubleV)
    unsafeRowWriter.write(20, doubleV)
    unsafeRowWriter.write(21, doubleV)
    unsafeRowWriter.write(22, doubleV)
    // +9 ints + 1 long + 1 int + 12 double = 23 fields

    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}
