/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ClassUtil {

  public static <T> T cast(Object object) {
    return (T) object;
  }

  public static <T extends Serializable> T clone(T obj) {
    T cloneObj = null;
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream obs = new ObjectOutputStream(out);
      obs.writeObject(obj);
      obs.close();
      ByteArrayInputStream ios = new ByteArrayInputStream(out.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(ios);
      cloneObj = (T) ois.readObject();
      ois.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return cloneObj;
  }
}
