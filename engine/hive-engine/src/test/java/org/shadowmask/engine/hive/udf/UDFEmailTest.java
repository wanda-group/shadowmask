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
 * Test for UDFEmail.
 */
public class UDFEmailTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFEmailWithWrongParameter1() {
    UDFEmail udfEmail = new UDFEmail();
    Text email = new Text("zhangsan@gmail.com");
    IntWritable mode = new IntWritable(5);
    udfEmail.evaluate(email, mode);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFEmailWithWrongParameter2() {
    UDFEmail udfEmail = new UDFEmail();
    Text email = new Text("zhangsan@gmail");
    IntWritable mode = new IntWritable(2);
    udfEmail.evaluate(email, mode);
  }

  @Test
  public void testUDFEmail() {
    UDFEmail udfEmail = new UDFEmail();
    Text email = new Text("zhangsan@gmail.com");
    IntWritable mode = new IntWritable(0);
    Text result = udfEmail.evaluate(email, mode);
    assertEquals("zhangsan@gmail.com", result.toString());
    mode.set(1);
    result = udfEmail.evaluate(email, mode);
    assertEquals("*@gmail.com", result.toString());
    mode.set(2);
    result = udfEmail.evaluate(email, mode);
    assertEquals("*@*.com", result.toString());
    mode.set(3);
    result = udfEmail.evaluate(email, mode);
    assertEquals("*@*.*", result.toString());

    email = null;
    result = udfEmail.evaluate(email, mode);
    assertNull(result);
  }

}
