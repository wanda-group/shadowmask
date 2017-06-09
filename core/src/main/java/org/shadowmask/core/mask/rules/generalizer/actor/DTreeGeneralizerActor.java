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
package org.shadowmask.core.mask.rules.generalizer.actor;

import org.shadowmask.core.domain.tree.DomainTreeNode;
import org.shadowmask.core.domain.tree.LeafLocator;

public abstract class DTreeGeneralizerActor<IN, OUT>
    implements GeneralizerActor<IN, OUT> {

  private int level;
  private int maxLevel = Integer.MAX_VALUE;
  private int minLevel = 0;
  private LeafLocator<IN, DomainTreeNode> dTree;

  @Override public OUT generalize(IN in) {
    DomainTreeNode leaf = dTree.locate(in);
    if (leaf == null) {
      return null;
    }
    DomainTreeNode pointer = leaf;
    for (int i = Math.max(0, minLevel); i < Math.min(maxLevel, level); ++i) {
      if (pointer.getParent() != null) {
        pointer = pointer.getParent();
      } else {
        break;
      }
    }
    return parseNode(pointer);

  }

  public abstract DTreeGeneralizerActor<IN, OUT> newInstance();

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public DTreeGeneralizerActor<IN, OUT> updateLevel(int deltaLevel) {
    this.level = this.level + deltaLevel;
    if (this.level > this.maxLevel) {
      this.level = this.maxLevel;
    } else if (this.level < this.minLevel) {
      this.level = this.minLevel;
    }
    return this;
  }

  public DTreeGeneralizerActor<IN, OUT> withLevel(int level) {
    this.level = level;
    return this;
  }

  public DTreeGeneralizerActor<IN, OUT> withDTree(
      LeafLocator<IN, DomainTreeNode> dTree) {
    this.dTree = dTree;
    return this;
  }

  protected abstract OUT parseNode(DomainTreeNode tnode);

  public int getMaxLevel() {
    return maxLevel;
  }

  public void setMaxLevel(int maxLevel) {
    this.maxLevel = maxLevel;
  }

  public int getMinLevel() {
    return minLevel;
  }

  public void setMinLevel(int minLevel) {
    this.minLevel = minLevel;
  }

  public LeafLocator<IN, DomainTreeNode> getdTree() {
    return dTree;
  }

  public void setdTree(LeafLocator<IN, DomainTreeNode> dTree) {
    this.dTree = dTree;
  }
}
