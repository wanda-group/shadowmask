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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFHash.
 */
public class UDFHashTest {
  @Test
  public void testUDFHash() {
    UDFHash udfHash = new UDFHash();
    Text data1 = new Text("hello");
    IntWritable result = udfHash.evaluate(data1);
    assertEquals(data1.hashCode(), result.get());
    LongWritable data2 = new LongWritable(80000000000L);
    result = udfHash.evaluate(data2);
    assertEquals(data2.hashCode(), result.get());
    IntWritable data3 = new IntWritable(345);
    result = udfHash.evaluate(data3);
    assertEquals(data3.hashCode(), result.get());

    data3 = null;
    result = udfHash.evaluate(data3);
    assertNull(result);
  }
}
