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

package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.util.DiscoveryUtil;

/**
 * This rule would evaluate whether the column value is a chinese phone number.
 */
public class PhoneRule extends QusiIdentifierRule {
  public PhoneRule(RuleContext ruleContext) {
    super(ruleContext);
  }

  @Override public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException(
          "Should fill the column value before fire inspect rules.");
    }

    String subs[] = value.split("\\-");
    // there are 2 parts after splitting by '-', e.g. 021-88888888
    if (subs.length != 2) {
      return false;
    }
    // 1. all parts consist of only digits;
    // 2. the first part should be 2, 3 or 4 digits long
    // 3. the second part should be 7 or 8 digits long
    if (subs[0].length() > 4 || subs[0].length() < 2) {
      return false;
    }
    if (subs[1].length() > 8 || subs[1].length() < 7) {
      return false;
    }
    for (String sub : subs) {
      for (int i = 0; i < sub.length(); i++) {
        if (DiscoveryUtil.isDigitChar(sub.charAt(i)) == false) {
          return false;
        }
      }
    }
    return true;
  }
}
