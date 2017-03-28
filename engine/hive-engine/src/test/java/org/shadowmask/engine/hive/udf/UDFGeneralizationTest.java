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

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFGeneralization.
 */
public class UDFGeneralizationTest {
  @Test(expected = MaskRuntimeException.class)
  public void testUDFGeneralizationLongWithWrongParameter1() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    LongWritable data = new LongWritable(45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationLongWithWrongParameter2() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    LongWritable data = new LongWritable(45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test
  public void testUDFGeneralizationLong() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    LongWritable data = new LongWritable(45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    LongWritable result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    data = null;
    result = udfGeneralization.evaluate(data, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationIntWithWrongParameter1() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    IntWritable data = new IntWritable(45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationIntWithWrongParameter2() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    IntWritable data = new IntWritable(45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test
  public void testUDFGeneralizationInt() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    IntWritable data = new IntWritable(45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    IntWritable result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    data = null;
    result = udfGeneralization.evaluate(data, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationShortWithWrongParameter1() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    ShortWritable data = new ShortWritable((short)45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationShortWithWrongParameter2() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    ShortWritable data = new ShortWritable((short)45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test
  public void testUDFGeneralizationShort() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    ShortWritable data = new ShortWritable((short)45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    ShortWritable result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    data = null;
    result = udfGeneralization.evaluate(data, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationByteWithWrongParameter1() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    ByteWritable data = new ByteWritable((byte)45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationByteWithWrongParameter2() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    ByteWritable data = new ByteWritable((byte) 45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfGeneralization.evaluate(data, level, unit);
  }

  @Test
  public void testUDFGeneralizationByte() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    ByteWritable data = new ByteWritable((byte)45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    ByteWritable result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfGeneralization.evaluate(data, level, unit);
    assertEquals(0, result.get());

    data = null;
    result = udfGeneralization.evaluate(data, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFGeneralizationTextWithWrongParameter1() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    Text data = new Text("hello world");
    IntWritable level = new IntWritable(-4);
    udfGeneralization.evaluate(data, level);
  }

  @Test
  public void testUDFGeneralizationText() {
    UDFGeneralization udfGeneralization = new UDFGeneralization();
    Text data = new Text("hello world");
    IntWritable level = new IntWritable(0);
    Text result = udfGeneralization.evaluate(data, level);
    assertEquals("hello world", result.toString());
    level.set(1);
    result = udfGeneralization.evaluate(data, level);
    assertEquals("hello worl", result.toString());
    level.set(5);
    result = udfGeneralization.evaluate(data, level);
    assertEquals("hello ", result.toString());
    level.set(10);
    result = udfGeneralization.evaluate(data, level);
    assertEquals("h", result.toString());
    level.set(11);
    result = udfGeneralization.evaluate(data, level);
    assertEquals("*", result.toString());
    level.set(20);
    result = udfGeneralization.evaluate(data, level);
    assertEquals("*", result.toString());

    data = null;
    result = udfGeneralization.evaluate(data, level);
    assertNull(result);
  }
}
