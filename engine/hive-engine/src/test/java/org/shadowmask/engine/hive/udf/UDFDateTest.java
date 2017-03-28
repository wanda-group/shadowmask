/**
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

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.DateTime;
import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;

import java.sql.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFDate.
 */
public class UDFDateTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFDateWithInvalidParameter1() {
    UDFDate udfDate = new UDFDate();
    Date d = new Date(DateTime.parse("2016-09-18").getMillis());
    DateWritable date = new DateWritable(d);
    IntWritable mask = new IntWritable(4);
    DateWritable result = udfDate.evaluate(date, mask);
  }

  @Test(expected = MaskRuntimeException.class)
  public void testUDFDateWithInvalidParameter2() {
    UDFDate udfDate = new UDFDate();
    Date d = new Date(DateTime.parse("2016-09-18").getMillis());
    DateWritable date = new DateWritable(d);
    IntWritable mask = new IntWritable(-1);
    DateWritable result = udfDate.evaluate(date, mask);
  }

  @Test
  public void testUDFDate() {
    UDFDate udfDate = new UDFDate();
    Date d = new Date(DateTime.parse("2016-09-18").getMillis());
    DateWritable date = new DateWritable(d);
    IntWritable mask = new IntWritable(0);
    DateWritable result = udfDate.evaluate(date, mask);
    assertEquals(d, result.get());
    mask.set(1);
    result = udfDate.evaluate(date, mask);
    Date expect1 = new Date(DateTime.parse("2016-09-01").getMillis());
    assertEquals(expect1, result.get());
    mask.set(2);
    result = udfDate.evaluate(date, mask);
    Date expect2 = new Date(DateTime.parse("2016-01-01").getMillis());
    assertEquals(expect2, result.get());
    mask.set(3);
    result = udfDate.evaluate(date, mask);
    Date expect3 = new Date(DateTime.parse("1901-01-01").getMillis());
    assertEquals(expect3, result.get());

    date = null;
    result = udfDate.evaluate(date, mask);
    assertNull(result);
  }
}
