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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFCipher.
 */
public class UDFCipherTest {
  @Test (expected = MaskRuntimeException.class)
  public void testUDFCipherWithWrongParameter1() {
    UDFCipher udfCipher = new UDFCipher();
    IntWritable mode = new IntWritable(5);
    Text key = new Text("1234567812345678");
    Text content = new Text("hello world");
    udfCipher.evaluate(content, mode, key);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFCipherWithWrongParameter2() {
    UDFCipher udfCipher = new UDFCipher();
    IntWritable mode = new IntWritable(1);
    Text key = new Text("123456789");
    Text content = new Text("hello world");
    udfCipher.evaluate(content, mode, key);
  }

  @Test
  public void testUDFCipher() {
    UDFCipher udfCipher = new UDFCipher();
    IntWritable mode = new IntWritable(1);
    Text key = new Text("123456781234567*");
    Text content1 = new Text("hello world");
    Text encry1 = udfCipher.evaluate(content1, mode, key);
    Text encry3 = udfCipher.evaluate(content1, mode, key);
    Text content2 = new Text("desensitization");
    Text encry2 = udfCipher.evaluate(content2, mode, key);
    assertEquals(encry1.toString(), encry3.toString());
    assertNotEquals(encry1.toString(), encry2.toString());
    mode.set(2);
    Text decry1 = udfCipher.evaluate(encry1, mode, key);
    Text decry2 = udfCipher.evaluate(encry2, mode, key);
    assertEquals(decry1.toString(), content1.toString());
    assertEquals(decry2.toString(), content2.toString());

    content1 = null;
    Text result = udfCipher.evaluate(content1, mode, key);
    assertNull(result);
  }
}
