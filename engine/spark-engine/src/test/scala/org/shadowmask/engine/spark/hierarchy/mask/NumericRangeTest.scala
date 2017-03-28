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

import org.junit.Test
import org.junit.Assert._

class NumericRangeTest {

  @Test
  def testIntRange(): Unit = {
    val boundaries = Array[Int](0, 3, 6, 7, 19, 21)
    val intRange = new NumericRange[Int](boundaries)
    assertEquals(0, intRange.getRangeIndex(0))
    assertEquals(1, intRange.getRangeIndex(3))
    assertEquals(1, intRange.getRangeIndex(4))
    assertEquals(2, intRange.getRangeIndex(6))
    assertEquals(3, intRange.getRangeIndex(7))
    assertEquals(3, intRange.getRangeIndex(8))
    assertEquals(4, intRange.getRangeIndex(19))
  }
}
