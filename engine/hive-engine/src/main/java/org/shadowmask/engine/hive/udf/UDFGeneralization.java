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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;
import org.shadowmask.core.mask.rules.generalizer.impl.*;


/**
 * UDFGeneralization
 */
@Description(name = "generalization",
        value = "_FUNC_(data,level,unit) - returns the generalization value for numerical or string data : "
                + "(1) Numerical data : generalized by unit^level; (2) String data : generalized by level\n"
                + "level - generalization level\n"
                + "unit - generalization unit: must be a positive integer",
        extended = "Example:\n"
                + "generalization(45,0,10) = 45;\n"
                + "generalization(45,1,10) = 40;\n"
                + "generalization(45,2,10) = 0;\n"
                + "generalization(45,1,7) = 42;\n"
                + "generalization('hello world',4) = 'hell'")
public class UDFGeneralization extends UDF {
  /**
   * Integer version
   */
  public IntWritable evaluate(IntWritable data, IntWritable level, IntWritable unit) {
    if (data == null) {
      return null;
    }
    int dataVal = data.get();
    int levelVal = level.get();
    int unitVal = unit.get();
    Generalizer<Integer, Integer> generalizer = new IntGeneralizer(Integer.MAX_VALUE, unitVal);
    IntWritable result = new IntWritable(generalizer.generalize(dataVal, levelVal));
    return result;
  }

  /**
   * Byte version
   */
  public ByteWritable evaluate(ByteWritable data, IntWritable level, IntWritable unit) {
    if (data == null) {
      return null;
    }
    byte dataVal = data.get();
    int levelVal = level.get();
    int unitVal = unit.get();
    Generalizer<Byte, Byte> generalizer = new ByteGeneralizer(Integer.MAX_VALUE, unitVal);
    ByteWritable result = new ByteWritable(generalizer.generalize(dataVal, levelVal));
    return result;
  }

  /**
   * Long version
   */
  public LongWritable evaluate(LongWritable data, IntWritable level, IntWritable unit) {
    if (data == null) {
      return null;
    }
    long dataVal = data.get();
    int levelVal = level.get();
    int unitVal = unit.get();
    Generalizer<Long, Long> generalizer = new LongGeneralizer(Integer.MAX_VALUE, unitVal);
    LongWritable result = new LongWritable(generalizer.generalize(dataVal, levelVal));
    return result;
  }

  /**
   * Short version
   */
  public ShortWritable evaluate(ShortWritable data, IntWritable level, IntWritable unit) {
    if (data == null) {
      return null;
    }
    short dataVal = data.get();
    int levelVal = level.get();
    int unitVal = unit.get();
    Generalizer<Short, Short> generalizer = new ShortGeneralizer(Integer.MAX_VALUE, unitVal);
    ShortWritable result = new ShortWritable(generalizer.generalize(dataVal, levelVal));
    return result;
  }

  /**
   * String version
   */
  public Text evaluate(Text data, IntWritable level) {
    if (data == null) {
      return null;
    }
    String dataVal = data.toString();
    int levelVal = level.get();
    Generalizer<String, String> generalizer = new StringGeneralizer(Integer.MAX_VALUE);
    Text result = new Text(generalizer.generalize(dataVal, levelVal));
    return result;
  }
}

