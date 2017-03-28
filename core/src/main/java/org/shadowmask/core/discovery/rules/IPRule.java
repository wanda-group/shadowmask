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
 * This rule would evaluate whether the column value is an IP address.
 */
public class IPRule extends QusiIdentifierRule {
  public IPRule(RuleContext ruleContext) {
    super(ruleContext);
  }

  @Override public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException(
          "Should fill the column value before fire inspect rules.");
    }

    String subs[] = value.split("\\.");
    // there are 4 parts after split by '.'
    if (subs.length != 4) {
      return false;
    }
    // all parts consist of only digits and the value of it is between 0 and 255
    for (String sub : subs) {
      if (sub.length() == 0) {
        return false;
      }
      for (int i = 0; i < sub.length(); i++) {
        if (DiscoveryUtil.isDigitChar(sub.charAt(i)) == false) {
          return false;
        }
      }
      int subVal = Integer.parseInt(sub);
      if (subVal > 255) {
        return false;
      }
    }
    return true;
  }
}
