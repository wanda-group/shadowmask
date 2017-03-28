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
package org.shadowmask.engine.spark.hierarchy.mask

class NumericRange[T: Numeric](boundaries: Array[T]) extends Serializable {

  def getRangeIndex(input: T): Int = {
    binarySearch(boundaries, 0, boundaries.length, input)
  }

  private def binarySearch(boundaries: Array[T], start: Int, end: Int, input: T): Int = {

    val checkpoint = (end + start) / 2
    val compFloor: Int = implicitly[Numeric[T]].compare(boundaries(checkpoint), input)
    val compCeil: Int = implicitly[Numeric[T]].compare(boundaries(checkpoint + 1), input)
    if (compFloor == 0) {
      return checkpoint
    }
    if (compCeil == 0) {
      return checkpoint + 1
    }

    (compFloor > 0, compCeil > 0) match {
      case (true, true) =>
        binarySearch(boundaries, start, checkpoint, input)
      case (false, false) =>
        binarySearch(boundaries, checkpoint, end, input)
      case (false, true) =>
        checkpoint
      case _ =>
        throw new RuntimeException("bad boundaries:" + boundaries.mkString(","))
    }
  }
}
