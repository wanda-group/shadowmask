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
package org.shadowmask.api.programming.hierarchy;

import org.shadowmask.core.data.DataType;

public abstract class Hierarchy {

  private DataType dataType;

  public static <H extends Hierarchy> H create(DataType type) {
    H h = null;
    switch (type) {
    case STRING:
      h = (H) new ValueBasedHierarchy();
      break;
    default:
      h = (H) new IntervalBasedHierarchy().setDataType(type);
    }
    h.setDataType(type);
    return h;
  }

  public Hierarchy setDataType(DataType dataType) {
    this.dataType = dataType;
    return this;
  }

  /**
   * json string used to build taxonomy tree {@see org.shadowmask.core.domain.tree.TaxTree}
   *
   * @return
   */
  public abstract String hierarchyJson();

  public DataType dataType() {
    return this.dataType;
  }

}
