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
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.IntWritable;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;
import org.shadowmask.core.mask.rules.generalizer.impl.TimestampGeneralizer;

import java.sql.Date;

/**
 * UDFDate.
 */
@Description(name = "date",
             value = "_FUNC_(date, mask) - returns the masked value of date\n"
                + "date - original date type\n"
                + "mask - 0~3 to indicate which parts to be masked",
             extended = "Example: date = \"2016-09-18\"\n"
                + "timestamp(time, 0) = \"2016-09-18\"\n"
                + "timestamp(time, 1) = \"2016-09-01\"\n"
                + "timestamp(time, 2) = \"2016-01-01\"\n"
                + "timestamp(time, 3) = \"1901-01-01\"")
public class UDFDate extends UDF {
  public DateWritable evaluate(DateWritable date, IntWritable mask) {
    if (date == null || mask == null) return null;
    Date dateVal = date.get();
    int mode = mask.get();
    if (mode >= 0) {
      mode += 4;
    }
    DateWritable result = new DateWritable();
    Generalizer<Long, Long> generalizer = new TimestampGeneralizer();
    result.set(new Date(generalizer.generalize(dateVal.getTime(), mode)));
    return result;
  }
}
