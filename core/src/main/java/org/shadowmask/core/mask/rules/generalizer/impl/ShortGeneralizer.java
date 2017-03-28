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

public class ShortGeneralizer implements Generalizer<Short, Short> {

  private int rootLevel;
  private int genUnit;

  public ShortGeneralizer() {
    this(String.valueOf(Short.MAX_VALUE).length());
  }

  public ShortGeneralizer(int rootHierarchyLevel) {
    this(rootHierarchyLevel, 10);
  }

  public ShortGeneralizer(int rootHierarchyLevel, int genUnit) {
    if(genUnit <= 0) {
      throw new MaskRuntimeException("Unit must be a positive integer, invalid genUnit = " + genUnit);
    }
    this.rootLevel = rootHierarchyLevel;
    this.genUnit = genUnit;
  }

  @Override public Short generalize(Short input, int hierarchyLevel) {
    if (hierarchyLevel > rootLevel || hierarchyLevel < 0) {
      throw new MaskRuntimeException(
          "Root hierarchy level of ShortGeneralizer is " + rootLevel +
              ", invalid input hierarchy level[" + hierarchyLevel + "]");
    }

    if (hierarchyLevel == 0) {
      return input;
    }

    int genSplit = 1;
    for(int i=0; i<hierarchyLevel; i++) {
      if(genSplit > input || genSplit >= Integer.MAX_VALUE/genUnit) {
        return 0;
      }
      genSplit = genSplit * genUnit;
    }

    return (short) (input - input % genSplit);
  }

  @Override public int getRootLevel() {
    return rootLevel;
  }
}
