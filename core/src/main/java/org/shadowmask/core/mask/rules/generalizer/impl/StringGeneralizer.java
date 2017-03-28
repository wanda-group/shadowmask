/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.core.mask.rules.generalizer.impl;

import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;

public class StringGeneralizer implements Generalizer<String, String> {

  private int rootLevel;

  public StringGeneralizer(int hierarchyLevel) {
    this.rootLevel = hierarchyLevel;
  }

  @Override public String generalize(String input, int hierarchyLevel) {
    if (input == null) {
      return null;
    }

    if (hierarchyLevel > rootLevel || hierarchyLevel < 0) {
      throw new MaskRuntimeException(
          "Root hierarchy level of StringGeneralizer is " + rootLevel +
              ", invalid input hierarchy level[" + hierarchyLevel + "]");
    }

    if (hierarchyLevel == 0) {
      return input;
    }

    if (input.length() <= hierarchyLevel) {
      return "*";
    }

    return input.substring(0, input.length() - hierarchyLevel);
  }

  @Override public int getRootLevel() {
    return rootLevel;
  }
}

