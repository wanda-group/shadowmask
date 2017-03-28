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
 * This rule would evaluate whether the column value is chinese citizen id.
 */
public class IDRule extends IdentifierRule {

  public IDRule(RuleContext context) {
    super(context);
  }

  @Override public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException(
          "Should fill the column value before fire inspect rules.");
    }

    if (value.length() == 18) {
      boolean result = true;
      int i;
      for (i = 0; i < value.length() - 1; i++) {
        result = result && DiscoveryUtil.isDigitChar(value.charAt(i));
      }
      // last digit of ID code may be 'x' or 'X'
      result = result && (DiscoveryUtil.isDigitChar(value.charAt(i)) ||
          value.charAt(i) == 'x' || value.charAt(i) == 'X');
      return result;
    }

    return false;
  }

}
