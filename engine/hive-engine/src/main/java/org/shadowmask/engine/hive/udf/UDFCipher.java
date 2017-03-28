/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import org.shadowmask.core.mask.rules.suppressor.impl.AESSuppressor;

/**
 * UDFCipher with MD5.
 *
 */
@Description(name = "encrytion",
        value = "_FUNC_(x,mode,key) - returns the encrypted/decrypted data string of x\n"
                + "mode - 1 : encryption, 2 : decryption\n"
                + "key - the key string for encryption/decryption (it's length must be 16)",
        extended = "Example:\n")
public class UDFCipher extends UDF {
    /**
     * mode:
     *   1 encryption
     *   2 decryption
     */
    public Text evaluate(Text content, IntWritable mode, Text key) {
        if (content == null) return null;

        AESSuppressor aes = new AESSuppressor();
        aes.initiate(mode.get(), key.toString());
        String res = aes.suppress(content.toString());
        return new Text(res);
    }
}

