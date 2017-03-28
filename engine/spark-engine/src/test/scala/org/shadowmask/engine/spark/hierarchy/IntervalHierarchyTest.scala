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
package org.shadowmask.engine.spark.hierarchy

import org.junit.Test
import org.junit.Assert._
import org.shadowmask.engine.spark.hierarchy.impl.IntervalHierarchy

class IntervalHierarchyTest {

  @Test
  def testIntRangeHierarchyTest(): Unit = {
    val hierarchy = new IntervalHierarchy(1, 20, 3, 3)
    val rule1: (Int) => Int = hierarchy.getMaskRule(1)
    assertEquals(0, rule1(2))
    assertEquals(2, rule1(8))
    val rule2 = hierarchy.getMaskRule(2)
    assertEquals(0, rule2(8))
    assertEquals(1, rule2(12))
  }

  @Test
  def testLongRangeHierarchyTest(): Unit = {
    val hierarchy = new IntervalHierarchy(1L, 20L, 3L, 3)
    val rule1: (Long) => Int = hierarchy.getMaskRule(1)
    assertEquals(0, rule1(2L))
    assertEquals(2, rule1(8L))
    val rule2: (Long) => Int = hierarchy.getMaskRule(2)
    assertEquals(0, rule2(8L))
    assertEquals(1, rule2(12L))
  }

  @Test
  def testDoubleRangeHierarchyTest(): Unit = {
    val hierarchy = new IntervalHierarchy(1.1D, 20.2D, 3.0D, 3)
    val rule1: (Double) => Int = hierarchy.getMaskRule(1)
    assertEquals(0, rule1(2.2D))
    assertEquals(2, rule1(8.3D))
    val rule2 = hierarchy.getMaskRule(2)
    assertEquals(0, rule2(8.2D))
    assertEquals(1, rule2(12.2D))
  }
}
