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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.generalizer.impl.IPGeneralizer;

/**
 * UDFIP.
 *
 */
@Description(name = "ip",
             value = "_FUNC_(ip, mask) - returns the masked value of ip\n"
                + "ip - original ip like x.x.x.x\n"
                + "mask - mask level (0~4)",
             extended = "Example:\n"
                + "ip('10.10.10.10', 0) = '10.10.10.10'"
                + "ip('10.10.10.10', 1) = '10.10.10.*'"
                + "ip('10.10.10.10', 2) = '10.10.*.*'"
                + "ip('10.10.10.10', 3) = '10.*.*.*'"
                + "ip('10.10.10.10', 4) = '*.*.*.*'")
public class UDFIP extends UDF {

  public Text evaluate(Text ip, IntWritable mask) {
    if (ip == null || mask == null) return null;
    int mode = mask.get();
    String str_ip = ip.toString();

    Text result = new Text();
    IPGeneralizer me = new IPGeneralizer();
    result.set(me.generalize(str_ip, mode));
    return result;
  }
}
