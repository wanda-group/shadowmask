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

package org.shadowmask.core.mask.rules.generalizer.impl;

import org.apache.commons.lang3.StringUtils;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;

/**
 * IPGeneralizer support 5 mask levels, take "10.191.192.11" for example:
 * - LEVEL0, 10.191.192.11
 * - LEVEL1, 10.191.192.*
 * - LEVEL2, 10.191.*.*
 * - LEVEL3, 10.*.*.*
 * - LEVEL4, *.*.*.*
 */
public class IPGeneralizer implements Generalizer<String, String> {
  private static final int ROOT_HIERARCHY_LEVEL = 4;

  @Override public String generalize(String ip, int hierarchyLevel) {
    if (ip == null) {
      return null;
    }

    if (hierarchyLevel > ROOT_HIERARCHY_LEVEL || hierarchyLevel < 0) {
      throw new MaskRuntimeException(
          "Root hierarchy level of IPGeneralizer is " + ROOT_HIERARCHY_LEVEL +
              ", invalid input hierarchy level[" + hierarchyLevel + "]");
    }

    if (hierarchyLevel == 0) {
      return ip;
    }

    String[] subs = ip.split("\\.");

    if (subs.length != ROOT_HIERARCHY_LEVEL) {
      throw new MaskRuntimeException("Invalid input ip to generalize:" + ip);
    }

    for (int i = 1; i <= hierarchyLevel; i++) {
      subs[ROOT_HIERARCHY_LEVEL - i] = "*";
    }

    return StringUtils.join(subs, ".");
  }

  @Override public int getRootLevel() {
    return ROOT_HIERARCHY_LEVEL;
  }
}
