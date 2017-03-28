/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.core.type;

import java.util.HashSet;
import java.util.Set;

/**
 * Define time format pattern.
 */
public class TimeFormatPattern {
  public static Set<String> patterns;
  static {
    patterns = new HashSet<String>();
    patterns.add("yyyy-MM-dd HH:mm:ss.SSS");
    patterns.add("yyyy.MM.dd HH:mm:ss.SSS");
    patterns.add("yyyy/MM/dd HH:mm:ss.SSS");
    patterns.add("yyyyMMdd HH:mm:ss.SSS");
    patterns.add("yyyy年MM月dd日 HH:mm:ss.SSS");
    patterns.add("yyyy-MM-dd HH:mm:ss");
    patterns.add("yyyy.MM.dd HH:mm:ss");
    patterns.add("yyyy/MM/dd HH:mm:ss");
    patterns.add("yyyyMMdd HH:mm:ss");
    patterns.add("yyyy年MM月dd日 HH:mm:ss");
  }

  public static void addPattern(String pattern) {
    if (pattern != null) {
      patterns.add(pattern);
    }
  }

  public static void removePattern(String pattern) {
    if (pattern != null) {
      patterns.remove(pattern);
    }
  }
}
