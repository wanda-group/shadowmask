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

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.DateTime;
import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFTimestamp.
 */
public class UDFTimestampTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFTimestampWithInvalidPatameter1() {
    UDFTimestamp udfTimestamp = new UDFTimestamp();
    long time = DateTime.parse("2016-09-18T10:30:32.222").getMillis();
    TimestampWritable timestamp = new TimestampWritable();
    timestamp.setTime(time);
    IntWritable mask = new IntWritable(-1);
    udfTimestamp.evaluate(timestamp, mask);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFTimestampWithInvalidPatameter2() {
    UDFTimestamp udfTimestamp = new UDFTimestamp();
    long time = DateTime.parse("2016-09-18T10:30:32.222").getMillis();
    TimestampWritable timestamp = new TimestampWritable();
    timestamp.setTime(time);
    IntWritable mask = new IntWritable(8);
    udfTimestamp.evaluate(timestamp, mask);
  }

  @Test
  public void testUDFTimestamp() {
    UDFTimestamp udfTimestamp = new UDFTimestamp();
    long time = DateTime.parse("2016-09-18T10:30:32.222").getMillis();
    TimestampWritable timestamp = new TimestampWritable();
    timestamp.setTime(time);
    IntWritable mask = new IntWritable(0);
    TimestampWritable result = udfTimestamp.evaluate(timestamp, mask);
    assertEquals(time, result.getTimestamp().getTime());
    mask.set(1);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect1 = DateTime.parse("2016-09-18T10:30:32.000").getMillis();
    assertEquals(expect1, result.getTimestamp().getTime());
    mask.set(2);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect2 = DateTime.parse("2016-09-18T10:30:00.000").getMillis();
    assertEquals(expect2, result.getTimestamp().getTime());
    mask.set(3);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect3 = DateTime.parse("2016-09-18T10:00:00.000").getMillis();
    assertEquals(expect3, result.getTimestamp().getTime());
    mask.set(4);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect4 = DateTime.parse("2016-09-18T00:00:00.000").getMillis();
    assertEquals(expect4, result.getTimestamp().getTime());
    mask.set(5);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect5 = DateTime.parse("2016-09-01T00:00:00.000").getMillis();
    assertEquals(expect5, result.getTimestamp().getTime());
    mask.set(6);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect6 = DateTime.parse("2016-01-01T00:00:00.000").getMillis();
    assertEquals(expect6, result.getTimestamp().getTime());
    mask.set(7);
    result = udfTimestamp.evaluate(timestamp, mask);
    long expect7 = DateTime.parse("1901-01-01T00:00:00.000").getMillis();
    assertEquals(expect7, result.getTimestamp().getTime());

    timestamp = null;
    result = udfTimestamp.evaluate(timestamp, mask);
    assertNull(result);
  }
}
