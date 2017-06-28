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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
      String valuei = values[i].trim();
      if (pointer.children == null) {
        pointer.children = new HashSet<>();
        pointer.nodeMap = new HashMap<>();
      }
      ValueNode nextNode = null;
      if (!pointer.nodeMap.containsKey(valuei)) {
        nextNode = new ValueNode();
        nextNode.text = valuei;
        pointer.nodeMap.put(valuei, nextNode);
        pointer.children.add(nextNode);
      } else {
        nextNode = pointer.nodeMap.get(valuei);
      }
      pointer = nextNode;
    }
    return this;
  }

  /**
   * add batch
   *
   * @param iterator
   * @return
   */
  public ValueBasedHierarchy addBatch(Iterator<String[]> iterator) {
    if (iterator != null) {
      while (iterator.hasNext()) {
        this.add(iterator.next());
      }
    }
    return this;
  }

  public ValueBasedHierarchy addBatch(Iterator<String> iterator,
      String separator) {
    if (iterator != null) {
      while (iterator.hasNext()) {
        this.add(iterator.next().split(separator));
      }
    }
    return this;
  }

  public ValueBasedHierarchy addBatch(final String[][] values) {
    if (values == null) {
      return this;
    }
    this.addBatch(new Iterator<String[]>() {
      int i = 0;

      @Override public boolean hasNext() {
        return i < values.length;
      }

      @Override public String[] next() {
        return values[i++];
      }

      @Override public void remove() {

      }
    });
    return this;
  }

  public ValueBasedHierarchy addBatch(final String[] values,
      final String separator) {
    if (values == null) {
      return this;
    }
    this.addBatch(new Iterator<String>() {
      int i = 0;

      @Override public boolean hasNext() {
        return i < values.length;
      }

      @Override public String next() {
        return values[i++];
      }

      @Override public void remove() {
      }
    }, separator);
    return this;
  }

  public ValueBasedHierarchy addBatch(InputStream in, String separator)
      throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String lineTxt = null;
    while ((lineTxt = br.readLine()) != null) {
      if (lineTxt.trim().equals("")) {
        continue;
      }
      String[] names = lineTxt.split(separator);
      this.add(names);
    }
    return this;
  }

  public ValueBasedHierarchy addBatch(File file, String separator)
      throws IOException {
    return this.addBatch(new FileInputStream(file), separator);
  }

  public ValueBasedHierarchy addBatch(String filePath, String separator)
      throws IOException {
    return this.addBatch(new File(filePath), separator);
  }

  @Override public String hierarchyJson() {
    ValueNode node = this.rootNode;
    if (node.children != null && node.children.size() == 1) {
      node = node.children.iterator().next();
    }
    return JsonUtil.newGsonInstance().toJson(new HierarchyJsonObject(node));
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
