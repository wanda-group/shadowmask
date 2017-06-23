/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.engine.spark.hierarchy.impl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.shadowmask.engine.spark.hierarchy.Hierarchy
import org.shadowmask.engine.spark.hierarchy.mask.MappingRule

class ArrayHierarchy(val hierarchyArray: Array[Array[String]]) extends Hierarchy[String, String]{

  override def rootHierarchyLevel: Int = hierarchyArray(0).length

  override def getUDF(hierarchy: Int): UserDefinedFunction = udf(getMaskRule(hierarchy))

  def getMaskRule(level: Int): (String) => String = {
    val mappingData: Array[(String, String)] = hierarchyArray.map {
      case array: Array[String] =>
        (array(0), array(level))
    }
    new MappingRule[String](mappingData).mask
  }
}
