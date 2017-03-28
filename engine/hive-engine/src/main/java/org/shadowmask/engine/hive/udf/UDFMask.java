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
import org.shadowmask.core.mask.rules.generalizer.Generalizer;
import org.shadowmask.core.mask.rules.generalizer.impl.ShadeGeneralizer;


/**
 * UDFMask.
 */
@Description(name = "mask",
        value = "_FUNC_(data,level,tag) - mask the last 'level' characters of 'data' with 'tag'\n"
                + "level - mask level\n"
                + "tag - the character to replace the sub-text",
        extended = "Example:\n"
                + "mask('hello world',2,'*') = 'hello wor**'\n"
                + "mask('hello world',10,'-') = 'h----------'\n")
public class UDFMask extends UDF{
  private final Text result = new Text();

  public Text evaluate(Text data, IntWritable level, Text tag) {
    // returns null when input is null
    if(data == null){
      return null;
    }

    Generalizer<String, String> generalizer = new ShadeGeneralizer(Integer.MAX_VALUE, tag.toString().charAt(0));

    result.set(generalizer.generalize(data.toString(), level.get()));
    return result;
  }
}
