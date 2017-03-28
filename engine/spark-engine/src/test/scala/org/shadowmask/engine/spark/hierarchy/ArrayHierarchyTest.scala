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
import org.shadowmask.engine.spark.hierarchy.impl.ArrayHierarchy


class ArrayHierarchyTest {

  @Test
  def testArrayHierarchy(): Unit = {
    val data = Array(Array("1234", "123*", "12**", "1***", "****"), Array("4321", "432*", "43**", "4***", "****"))
    val hierarchy: ArrayHierarchy = new ArrayHierarchy(data)
    val rule1: (String) => String = hierarchy.getMaskRule(1)
    assertEquals("123*", rule1("1234"))
    val rule2 = hierarchy.getMaskRule(3)
    assertEquals("4***", rule2("4321"))
  }
}
