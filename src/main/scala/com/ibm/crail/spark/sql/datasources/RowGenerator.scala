package com.ibm.crail.spark.sql.datasources

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
  * Created by atr on 10.08.17.
  */

trait RowGenerator {
  def nextRow():UnsafeRow
}