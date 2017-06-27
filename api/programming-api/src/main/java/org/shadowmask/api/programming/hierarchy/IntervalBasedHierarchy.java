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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.util.JsonUtil;
import org.shadowmask.core.util.Predictor;

/**
 * Hierarchy base on intervals ,only support String type
 *
 * @param <T> : data type
 */
public class IntervalBasedHierarchy<T extends Comparable<T>>
    extends Hierarchy {

  public List<IntervalNode> intervals = new LinkedList<>();

  public List<LevelGroup> groups = new ArrayList<>();

  private int currentLevel = 0;

  private IntervalNode root;

  private DataType dataType;

  public IntervalBasedHierarchy setDataType(DataType dataType) {
    this.dataType = dataType;
    return this;
  }

  @Override public DataType dataType() {
    return this.dataType;
  }

  @Override public String hierarchyJson() {
    check();
    List<IntervalNode> nodeList = new ArrayList<>();
    for (IntervalNode node : intervals) {
      nodeList.add(node);
    }
    List<IntervalNode> higherList = null;
    for (LevelGroup group : groups) {
      higherList = new ArrayList<>();
      int indexPadding = 0;
      for (LevelNode levelNode : group.nodes) {
        IntervalNode node = new IntervalNode();
        node.text = levelNode.text;
        node.lBound = nodeList.get(indexPadding).lBound;
        node.hBound = nodeList.get(indexPadding + levelNode.size - 1).hBound;
        node.children =
            nodeList.subList(indexPadding, indexPadding + levelNode.size);
        indexPadding += levelNode.size;
        higherList.add(node);
      }
      nodeList = higherList;
    }
    if (nodeList.size() == 1) {
      this.root = nodeList.get(0);
    } else {
      this.root = new IntervalNode();
      this.root.text = "*";
      this.root.lBound = nodeList.get(0).lBound;
      this.root.hBound = nodeList.get(nodeList.size() - 1).hBound;
      this.root.children = nodeList;
    }

    return JsonUtil.newGsonInstance()
        .toJson(new HierarchyJsonObject(this.root));
  }

  public LevelGroup level(int level) {
    Predictor.predict(level <= currentLevel,
        "can not get level bigger than current level");
    LevelGroup group = null;
    if (level == currentLevel) {
      group = new LevelGroup();
      groups.add(group);
      ++currentLevel;
    } else {
      group = groups.get(level);
    }
    return group;
  }

  public IntervalBasedHierarchy clearIntervals() {
    this.intervals.clear();
    return this;
  }

  public IntervalBasedHierarchy clearGroups() {
    this.groups.clear();
    return this;
  }

  public IntervalBasedHierarchy clearAll() {
    this.clearGroups();
    this.clearIntervals();
    return this;
  }

  public IntervalBasedHierarchy addInterval(T lBound, T hBound, String name) {
    IntervalNode node = new IntervalNode();
    node.lBound = lBound;
    node.hBound = hBound;
    node.text = name;
    intervals.add(node);
    return this;
  }

  public IntervalBasedHierarchy check() {
    this.checkIntervals();
    this.checkGroups();
    return this;
  }

  public IntervalBasedHierarchy checkIntervals() {
    Predictor.predict(this.intervals != null && this.intervals.size() > 0,
        "intervals should not be null or empty list");
    IntervalNode node = this.intervals.get(0);
    for (int i = 1; i < this.intervals.size(); i++) {
      Predictor.predict(node.hBound.compareTo(node.lBound) >= 0,
          "high bound little than low bound");

      Predictor.predict(
          this.intervals.get(i).hBound.compareTo(this.intervals.get(i).lBound)
              >= 0, "high bound little than low bound");

      Predictor
          .predict(this.intervals.get(i).lBound.compareTo(node.hBound) >= 0,
              "interval step should increasing");
      node = this.intervals.get(i);
    }

    return this;
  }

  private void checkGroup(LevelGroup group, int levelSize) {
    Predictor.predict(group.nodes.size() < levelSize,
        "high level size should little than low level size");
    int num = 0;
    for (LevelNode node : group.nodes) {
      num += node.size;
    }
    Predictor.predict(num == levelSize,
        "high level size sum should equals low level size");
  }

  public IntervalBasedHierarchy checkGroups() {
    if (this.groups == null || this.groups.size() == 0) {
      return this;
    }
    int levelSize = this.intervals.size();
    for (LevelGroup group : this.groups) {
      Predictor.predict(group != null, "group should not be null");
      checkGroup(group, levelSize);
      levelSize = group.nodes.size();
    }
    return this;
  }

  class HierarchyJsonObject {
    public String version = "1.0";
    public IntervalNode root;

    public HierarchyJsonObject(IntervalNode root) {
      this.root = root;
    }
  }

  protected class IntervalNode {
    public String text;
    public T lBound;
    public T hBound;
    public List<IntervalNode> children;
  }

  private class LevelNode {
    public int size;
    public String text;

    public LevelNode(int size, String text) {
      this.size = size;
      this.text = text;
    }
  }

  public class LevelGroup {
    public List<LevelNode> nodes = new ArrayList<>();

    public LevelGroup addGroup(int size, String name) {
      nodes.add(new LevelNode(size, name));
      return this;
    }
  }

}
