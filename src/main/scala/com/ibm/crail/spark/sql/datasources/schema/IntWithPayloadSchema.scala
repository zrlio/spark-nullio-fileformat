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
