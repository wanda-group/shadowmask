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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFIP.
 */
public class UDFIPTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFIPWithInvalidParameter1() {
    UDFIP udfip = new UDFIP();
    Text ip = new Text("10.199.192.11");
    IntWritable mask = new IntWritable(-1);
    udfip.evaluate(ip, mask);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFIPWithInvalidParameter2() {
    UDFIP udfip = new UDFIP();
    Text ip = new Text("10.199.192.11");
    IntWritable mask = new IntWritable(5);
    udfip.evaluate(ip, mask);
  }

  @Test
  public void testUDFIP() {
    UDFIP udfip = new UDFIP();
    Text ip = new Text("10.199.192.11");
    IntWritable mask = new IntWritable(0);
    Text result = udfip.evaluate(ip, mask);
    assertEquals("10.199.192.11", result.toString());
    mask.set(1);
    result = udfip.evaluate(ip, mask);
    assertEquals("10.199.192.*", result.toString());
    mask.set(2);
    result = udfip.evaluate(ip, mask);
    assertEquals("10.199.*.*", result.toString());
    mask.set(3);
    result = udfip.evaluate(ip, mask);
    assertEquals("10.*.*.*", result.toString());
    mask.set(4);
    result = udfip.evaluate(ip, mask);
    assertEquals("*.*.*.*", result.toString());

    ip = null;
    result = udfip.evaluate(ip, mask);
    assertNull(result);
  }
}
