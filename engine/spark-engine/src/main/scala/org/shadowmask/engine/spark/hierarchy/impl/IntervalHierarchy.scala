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

import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.shadowmask.engine.spark.hierarchy.Hierarchy
import org.shadowmask.engine.spark.hierarchy.mask.RangeRule

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class IntervalHierarchy[T: Numeric : ClassTag : TypeTag](bottom: T, top: T, step: T, level: Int) extends Hierarchy[T, Int] {
  override def rootHierarchyLevel: Int = level

  override def getUDF(hierarchy: Int): UserDefinedFunction = udf(getMaskRule(hierarchy))

  def getMaskRule(hierarchy: Int): (T) => Int = {
    val list = new mutable.MutableList[T]
    var boundary = bottom
    val num = implicitly[Numeric[T]]
    var cuStep = step
    for (i <- 1 until hierarchy) {
      cuStep = num.times(cuStep, step)
    }
    while (num.lt(boundary, top)) {
      list += boundary
      boundary = num.plus(boundary, cuStep)
    }
    list += top
    new RangeRule[T](list.toArray[T]).mask
  }
}
