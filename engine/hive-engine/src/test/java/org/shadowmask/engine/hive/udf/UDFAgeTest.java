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
import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for UDFAge
 */
public class UDFAgeTest {
  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeLongWithWrongParameter1() {
    UDFAge udfAge = new UDFAge();
    LongWritable age = new LongWritable(45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfAge.evaluate(age, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeLongWithWrongParameter2() {
    UDFAge udfAge = new UDFAge();
    LongWritable age = new LongWritable(45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfAge.evaluate(age, level, unit);
  }

  @Test
  public void testUDFAgeLong() {
    UDFAge udfAge = new UDFAge();
    LongWritable age = new LongWritable(45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    LongWritable result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    age = null;
    result = udfAge.evaluate(age, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeIntWithWrongParameter1() {
    UDFAge udfAge = new UDFAge();
    IntWritable age = new IntWritable(45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfAge.evaluate(age, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeIntWithWrongParameter2() {
    UDFAge udfAge = new UDFAge();
    IntWritable age = new IntWritable(45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfAge.evaluate(age, level, unit);
  }

  @Test
  public void testUDFAgeInt() {
    UDFAge udfAge = new UDFAge();
    IntWritable age = new IntWritable(45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    IntWritable result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    age = null;
    result = udfAge.evaluate(age, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeShortWithWrongParameter1() {
    UDFAge udfAge = new UDFAge();
    ShortWritable age = new ShortWritable((short)45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfAge.evaluate(age, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeShortWithWrongParameter2() {
    UDFAge udfAge = new UDFAge();
    ShortWritable age = new ShortWritable((short)45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfAge.evaluate(age, level, unit);
  }

  @Test
  public void testUDFAgeShort() {
    UDFAge udfAge = new UDFAge();
    ShortWritable age = new ShortWritable((short)45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    ShortWritable result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    age = null;
    result = udfAge.evaluate(age, level, unit);
    assertNull(result);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeByteWithWrongParameter1() {
    UDFAge udfAge = new UDFAge();
    ByteWritable age = new ByteWritable((byte)45);
    IntWritable level = new IntWritable(-4);
    IntWritable unit = new IntWritable(10);
    udfAge.evaluate(age, level, unit);
  }

  @Test (expected = MaskRuntimeException.class)
  public void testUDFAgeByteWithWrongParameter2() {
    UDFAge udfAge = new UDFAge();
    ByteWritable age = new ByteWritable((byte) 45);
    IntWritable level = new IntWritable(5);
    IntWritable unit = new IntWritable(-5);
    udfAge.evaluate(age, level, unit);
  }

  @Test
  public void testUDFAgeByte() {
    UDFAge udfAge = new UDFAge();
    ByteWritable age = new ByteWritable((byte)45);
    IntWritable level = new IntWritable(0);
    IntWritable unit = new IntWritable(10);
    ByteWritable result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(40, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    unit.set(3);
    level.set(0);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(1);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(2);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(45, result.get());
    level.set(3);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(27, result.get());
    level.set(4);
    result = udfAge.evaluate(age, level, unit);
    assertEquals(0, result.get());

    age = null;
    result = udfAge.evaluate(age, level, unit);
    assertNull(result);
  }
}
