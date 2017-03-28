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
import org.apache.hadoop.io.Text;
import org.shadowmask.core.mask.rules.suppressor.Suppressor;
import org.shadowmask.core.mask.rules.suppressor.impl.MappingSuppressor;
import org.shadowmask.core.mask.rules.suppressor.impl.UUIDSuppressor;

/**
 * UDFUIdentifier.
 *
 */
@Description(name = "uIdentifier",
        value = "_FUNC_(x) - returns a unique identifier of x using UUID\n"
                + "_FUNC_(x, 'MD5') - returns a unique identifier of x using MD5",
        extended = "Example:\n")
public class UDFUIdentifier extends UDF {
    /* Method-1: use UUID, the best efficiency*/
    public Text evaluate(Text content) {
        if (content == null) return null;
        String str = content.toString();

        Suppressor<Object, String> uid = new UUIDSuppressor();
        str = uid.suppress(str);

        return new Text(str);
    }

    /* Method-2: use MD5/SHA/..*/
    public Text evaluate(Text content, Text encryptor) {
        if (content == null) return null;
        String str = content.toString();

        Suppressor<Object, String> uid = new MappingSuppressor("MD5");
        String res = uid.suppress(str);

        if (res != null) return new Text(res);
        return null;
    }
}
