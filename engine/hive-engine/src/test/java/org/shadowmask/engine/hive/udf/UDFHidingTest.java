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
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for UDFHiding.
 */
public class UDFHidingTest {
  @Test
  public void testUDFHiding() {
    UDFHiding udfHiding = new UDFHiding();

    ByteWritable dataByte = new ByteWritable((byte)34);
    ByteWritable valueByte = new ByteWritable((byte)46);
    ByteWritable result1 = udfHiding.evaluate(dataByte, valueByte);
    assertEquals(valueByte.get(), result1.get());

    DoubleWritable dataDouble = new DoubleWritable(34.45);
    DoubleWritable valueDouble = new DoubleWritable(4.6);
    DoubleWritable result2 = udfHiding.evaluate(dataDouble, valueDouble);
    assertEquals(valueDouble.get(), result2.get(), Double.MIN_VALUE);

    FloatWritable dataFloat = new FloatWritable(34.45f);
    FloatWritable valueFloat = new FloatWritable(4.6f);
    FloatWritable result3 = udfHiding.evaluate(dataFloat, valueFloat);
    assertEquals(valueFloat.get(), result3.get(), Float.MIN_VALUE);

    IntWritable dataInt = new IntWritable(33456);
    IntWritable valueInt = new IntWritable(-4546);
    IntWritable result4 = udfHiding.evaluate(dataInt, valueInt);
    assertEquals(valueInt.get(), result4.get());

    LongWritable dataLong = new LongWritable(334563L);
    LongWritable valueLong = new LongWritable(-45465555L);
    LongWritable result5 = udfHiding.evaluate(dataLong, valueLong);
    assertEquals(valueLong.get(), result5.get());

    ShortWritable dataShort = new ShortWritable((short)33);
    ShortWritable valueShort = new ShortWritable((short)-4548);
    ShortWritable result6 = udfHiding.evaluate(dataShort, valueShort);
    assertEquals(valueShort.get(), result6.get());

    Text dataText = new Text("hello");
    Text valueText = new Text("intel");
    Text result7 = udfHiding.evaluate(dataText, valueText);
    assertEquals(valueText.toString(), result7.toString());
  }
}
