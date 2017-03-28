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
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;
import org.shadowmask.core.mask.rules.generalizer.impl.TimestampGeneralizer;

import java.sql.Timestamp;

/**
 * UDFTimestamp.
 *
 */
@Description(name = "timestamp",
             value = "_FUNC_(timestamp, mask) - returns the masked value of timestamp\n"
                + "timestamp - original timestamp type\n"
                + "mask - 0~7 to indicate which parts to be masked",
             extended = "Example: time = \"2016-09-18T10:30:32.222\"\n"
                + "timestamp(time, 0) = \"2016-09-18T10:30:32.222\"\n"
                + "timestamp(time, 1) = \"2016-09-18T10:30:32.000\"\n"
                + "timestamp(time, 2) = \"2016-09-18T10:30:00.000\"\n"
                + "timestamp(time, 3) = \"2016-09-18T10:00:00.000\"\n"
                + "timestamp(time, 4) = \"2016-09-18T00:00:00.000\"\n"
                + "timestamp(time, 5) = \"2016-09-01T00:00:00.000\"\n"
                + "timestamp(time, 6) = \"2016-01-01T00:00:00.000\"\n"
                + "timestamp(time, 7) = \"0000-01-01T00:00:00.000\"\n")
public class UDFTimestamp extends UDF {

  public TimestampWritable evaluate(TimestampWritable timestamp, IntWritable mask) {
    if (timestamp == null || mask == null) return null;
    int mode = mask.get();
    Timestamp ts = timestamp.getTimestamp();
    TimestampWritable result = new TimestampWritable();
    Generalizer<Long, Long> generalizer = new TimestampGeneralizer();
    result.set(new Timestamp(generalizer.generalize(ts.getTime(), mode)));
    return result;
  }

}
