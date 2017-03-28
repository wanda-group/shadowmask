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

package org.shadowmask.core.mask.rules.generalizer.impl;

import org.apache.commons.lang3.StringUtils;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;

/**
 * EmailGeneralizer support 4 mask levels, take "linda@gmail.com" for example:
 * - LEVEL0, linda@gmail.com
 * - LEVEL1, *@gmail.com
 * - LEVEL2, *@*.com
 * - LEVEL3, *@*.*
 */
public class EmailGeneralizer implements Generalizer<String, String> {

  private static int ROOT_HIERARCHY_LEVEL = 3;

  @Override public String generalize(String email, int hierarchyLevel) {
    if (email == null) {
      return null;
    }

    if (hierarchyLevel > ROOT_HIERARCHY_LEVEL || hierarchyLevel < 0) {
      throw new MaskRuntimeException(
          "Root hierarchy level of IPGeneralizer is " + ROOT_HIERARCHY_LEVEL +
              ", invalid input hierarchy level[" + hierarchyLevel + "]");
    }

    if (hierarchyLevel == 0) {
      return email;
    }

    String[] subs = email.split("\\@");
    if (subs.length != 2) {
      throw new MaskRuntimeException(
          "Invalid input email to generalize:" + email);
    }

    String[] domain = subs[1].split("\\.");
    if (domain.length != 2) {
      throw new MaskRuntimeException(
          "Invalid input email to generalize:" + email);
    }

    switch (hierarchyLevel) {
    case 1:
      subs[0] = "*";
      break;
    case 2:
      subs[0] = "*";
      domain[0] = "*";
      break;
    case 3:
      domain[1] = "*";
      domain[0] = "*";
      subs[0] = "*";
    }

    subs[1] = StringUtils.join(domain, ".");

    return StringUtils.join(subs, "@");
  }

  @Override public int getRootLevel() {
    return ROOT_HIERARCHY_LEVEL;
  }
}
