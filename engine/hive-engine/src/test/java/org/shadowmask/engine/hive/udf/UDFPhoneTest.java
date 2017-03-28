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
 * Test for UDFPhone.
 */
public class UDFPhoneTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFPhoneWithInvalidParameter1() {
    UDFPhone udfPhone = new UDFPhone();
    Text phone = new Text("021-66666666");
    IntWritable mask = new IntWritable(-1);
    udfPhone.evaluate(phone, mask);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFPhoneWithInvalidParameter2() {
    UDFPhone udfPhone = new UDFPhone();
    Text phone = new Text("021-66666666");
    IntWritable mask = new IntWritable(3);
    udfPhone.evaluate(phone, mask);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFPhoneWithInvalidParameter3() {
    UDFPhone udfPhone = new UDFPhone();
    Text phone = new Text("45678898");
    IntWritable mask = new IntWritable(1);
    udfPhone.evaluate(phone, mask);
  }

  @Test
  public void testUDFPhone() {
    UDFPhone udfPhone = new UDFPhone();
    Text phone = new Text("021-66666666");
    IntWritable mask = new IntWritable(0);
    Text result = udfPhone.evaluate(phone, mask);
    assertEquals("021-66666666", result.toString());
    mask = new IntWritable(1);
    result = udfPhone.evaluate(phone, mask);
    assertEquals("021-********", result.toString());
    mask = new IntWritable(2);
    result = udfPhone.evaluate(phone, mask);
    assertEquals("***-********", result.toString());

    phone = null;
    result = udfPhone.evaluate(phone, mask);
    assertNull(result);
  }
}
