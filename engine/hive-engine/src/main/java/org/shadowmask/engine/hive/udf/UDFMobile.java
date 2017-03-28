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
import org.shadowmask.core.mask.rules.generalizer.impl.MobileGeneralizer;

/**
 * UDFMobile.
 *
 */
@Description(name = "mobile",
             value = "_FUNC_(mobile, mask) - returns the masked value of mobile\n"
                + "mobile - original 11-digit mobile number combined with three parts \n"
                + "mask - 0~3 to indicate how many parts to be masked",
             extended = "Example:\n"
                + "mobile('13566668888', 0) = '13566668888'\n"
                + "mobile('13566668888', 1) = '1356666****'\n"
                + "mobile('13566668888', 2) = '135********'\n"
                + "mobile('13566668888', 3) = '***********'\n")
public class UDFMobile extends UDF {

  public Text evaluate(Text mobile, IntWritable mask) {
    if (mobile == null || mask == null) return null;
    int mode = mask.get();
    String str_mobile = mobile.toString();

    Text result = new Text();
    Generalizer<String, String> generalizer = new MobileGeneralizer();
    result.set(generalizer.generalize(str_mobile, mode));
    return result;
  }
}
