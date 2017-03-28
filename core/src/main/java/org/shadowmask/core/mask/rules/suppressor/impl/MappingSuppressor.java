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

package org.shadowmask.core.mask.rules.suppressor.impl;

import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.suppressor.Suppressor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/* Generate UID by method-2: use MD5/SHA/..*/
public class MappingSuppressor implements Suppressor<Object, String> {
  private String encryptor = "MD5";

  public MappingSuppressor(String encryptor) {
    if (encryptor != null)
      this.encryptor = encryptor;
  }

  @Override public String suppress(Object content) {
    if (content == null) {
      return null;
    }
    try {
      MessageDigest md = MessageDigest.getInstance(encryptor);
      md.update(content.toString().getBytes());
      byte[] eb = md.digest();
      md.reset();

      StringBuilder sb = new StringBuilder();
      for (byte b : eb) {
        sb.append(Integer.toHexString(b & 0xff));
      }

      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new MaskRuntimeException(
          "Failed to encrypt input data with [" + encryptor + "].");
    }
  }
}
