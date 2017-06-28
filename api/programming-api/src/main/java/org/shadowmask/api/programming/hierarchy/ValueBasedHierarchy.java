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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.util.JsonUtil;
import org.shadowmask.core.util.Predictor;

/**
 * Hierarchy base on values ,only support String type
 */
public class ValueBasedHierarchy extends Hierarchy {

  private ValueNode rootNode;

  /**
   * add value Hierarchy relation like
   *
   * @param values
   * @return
   */
  public ValueBasedHierarchy add(String... values) {
    Predictor.predict(values != null && values.length > 0,
        "hierarchy values should not be null");
    if (rootNode == null) {
      rootNode = new ValueNode();
      rootNode.text = "*";
    }
    ValueNode pointer = rootNode;
    for (int i = values.length - 1; i >= 0; i--) {
      if (pointer.children == null) {
        pointer.children = new HashSet<>();
        pointer.nodeMap = new HashMap<>();
      }
      ValueNode nextNode = null;
      if (!pointer.nodeMap.containsKey(values[i])) {
        nextNode = new ValueNode();
        nextNode.text = values[i];
        pointer.nodeMap.put(values[i], nextNode);
        pointer.children.add(nextNode);
      } else {
        nextNode = pointer.nodeMap.get(values[i]);
      }
      pointer = nextNode;
    }
    return this;
  }

  @Override public String hierarchyJson() {
    return JsonUtil.newGsonInstance()
        .toJson(new HierarchyJsonObject(this.rootNode));
  }

  @Override public DataType dataType() {
    return DataType.STRING;
  }

  class HierarchyJsonObject {
    public String version = "1.0";
    public ValueNode root;

    public HierarchyJsonObject(ValueNode root) {
      this.root = root;
    }
  }

  class ValueNode {
    public String text;
    public Set<ValueNode> children;
    transient public Map<String, ValueNode> nodeMap;

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ValueNode valueNode = (ValueNode) o;

      return text != null
          ? text.equals(valueNode.text)
          : valueNode.text == null;
    }

    @Override public int hashCode() {
      return text != null ? text.hashCode() : 0;
    }
  }
}
