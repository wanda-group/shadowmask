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
import org.shadowmask.core.mask.rules.generalizer.impl.EmailGeneralizer;

/**
 * UDFEmail.
 *
 */
@Description(name = "email",
             value = "_FUNC_(email, mask) - returns the masked value of email\n"
                + "email - original email string combined with an account name and an email domain like xx@yy.zz\n"
                + "mask - 4 kind of mask mode 0~3",
             extended = "Example:\n"
                + "email(xx@yy.zz,0) = xx@yy.zz\n"
                + "email(xx@yy.zz,1) = *@yy.zz\n"
                + "email(xx@yy.zz,2) = *@*.zz\n"
                + "email(xx@yy.zz,3) = *@*.*\n")
public class UDFEmail extends UDF {
  /* mask XXX@YYY:
   * 0-(00)2  XXX@YYY
   * 1-(01)2  XXX
   * 2-(10)2  YYY
   * 3-(11)2  
   */
  public Text evaluate(Text email, IntWritable mask) {
    if (email == null || mask == null) return null;
    int mode = mask.get();
    String str_email = email.toString();
    Text result = new Text();

    Generalizer<String, String> generalizer = new EmailGeneralizer();

    result.set(generalizer.generalize(str_email, mode));
    return result;
  }
}
