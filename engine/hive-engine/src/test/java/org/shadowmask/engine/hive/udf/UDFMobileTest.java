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
 * Test for UDFMobile.
 */
public class UDFMobileTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFMobileWithInvalidParameter1() {
    UDFMobile udfMobile = new UDFMobile();
    Text mobile = new Text("13566668888");
    IntWritable mask = new IntWritable(-1);
    udfMobile.evaluate(mobile, mask);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFMobileWithInvalidParameter2() {
    UDFMobile udfMobile = new UDFMobile();
    Text mobile = new Text("13566668888");
    IntWritable mask = new IntWritable(4);
    udfMobile.evaluate(mobile, mask);
  }

  @Test
  public void testUDFMobile() {
    UDFMobile udfMobile = new UDFMobile();
    Text mobile = new Text("13566668888");
    IntWritable mask = new IntWritable(0);
    Text result = udfMobile.evaluate(mobile, mask);
    assertEquals("13566668888", result.toString());
    mask.set(1);
    result = udfMobile.evaluate(mobile, mask);
    assertEquals("1356666****", result.toString());
    mask.set(2);
    result = udfMobile.evaluate(mobile, mask);
    assertEquals("135********", result.toString());
    mask.set(3);
    result = udfMobile.evaluate(mobile, mask);
    assertEquals("***********", result.toString());

    mobile = null;
    result = udfMobile.evaluate(mobile, mask);
    assertNull(result);
  }
}
