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
package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFUIdentifier.
 */
public class UDFUIdentifiertTest {
  @Test
  public void testUDFUIdentifier1() {
    UDFUIdentifier udfuIdentifier = new UDFUIdentifier();
    Text content1 = new Text("hello");
    Text content2 = new Text("hello");
    Text content3 = new Text("world");
    Text result1 = udfuIdentifier.evaluate(content1);
    Text result2 = udfuIdentifier.evaluate(content2);
    Text result3 = udfuIdentifier.evaluate(content3);
    assertEquals(result1.toString(), result2.toString());
    assertNotEquals(result1.toString(), result3.toString());

    content1 = null;
    result1 = udfuIdentifier.evaluate(content1);
    assertNull(result1);
  }

  @Test
  public void testUDFUIdentifier2() {
    UDFUIdentifier udfuIdentifier = new UDFUIdentifier();
    Text content1 = new Text("hello");
    Text content2 = new Text("hello");
    Text content3 = new Text("world");
    Text mode = new Text("MD5");
    Text result1 = udfuIdentifier.evaluate(content1, mode);
    Text result2 = udfuIdentifier.evaluate(content2, mode);
    Text result3 = udfuIdentifier.evaluate(content3, mode);
    assertEquals(result1.toString(), result2.toString());
    assertNotEquals(result1.toString(), result3.toString());

    content1 = null;
    result1 = udfuIdentifier.evaluate(content1, mode);
    assertNull(result1);
  }
}
