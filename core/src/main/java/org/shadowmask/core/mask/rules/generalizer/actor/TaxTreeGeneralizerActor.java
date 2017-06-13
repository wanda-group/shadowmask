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

import org.shadowmask.core.domain.tree.TaxTreeNode;
import org.shadowmask.core.domain.tree.LeafLocator;
import org.shadowmask.core.mask.rules.generalizer.functions.Function;
import org.shadowmask.core.util.ClassUtil;
import org.shadowmask.core.util.Predictor;

public class TaxTreeGeneralizerActor<IN, OUT>
    implements GeneralizerActor<IN, OUT> {

  private int level;
  private int maxLevel = Integer.MAX_VALUE;
  private int minLevel = 0;

  private Function<TaxTreeNode, OUT> resultParser =
      new Function<TaxTreeNode, OUT>() {
        @Override public OUT apply(TaxTreeNode input) {
          return (OUT) input.getName();
        }
      };

  private Function<IN, OUT> inputPaser = new Function<IN, OUT>() {
    @Override public OUT apply(IN input) {
      return (OUT) input.toString();
    }
  };

  private LeafLocator<IN> dTree;

  @Override public OUT generalize(IN in) {
    TaxTreeNode leaf = dTree.locate(in);
    if (leaf == null) {
      return inputPaser.apply(in);
    }

    if (level == 0) {
      return inputPaser.apply(in);
    }

    TaxTreeNode pointer = leaf;
    for (int i = Math.max(0, minLevel); i < Math.min(maxLevel, level - 1);
        ++i) {
      if (pointer.getParent() != null) {
        pointer = pointer.getParent();
      } else {
        break;
      }
    }
    return parseNode(pointer);

  }

  public TaxTreeGeneralizerActor<IN, OUT> newInstance() {
    return ClassUtil.<TaxTreeGeneralizerActor<IN, OUT>>cast(
        ClassUtil.clone(this));
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public TaxTreeGeneralizerActor<IN, OUT> updateLevel(int deltaLevel) {
    this.level = this.level + deltaLevel;
    if (this.level > this.maxLevel) {
      this.level = this.maxLevel;
    } else if (this.level < this.minLevel) {
      this.level = this.minLevel;
    }
    return this;
  }

  public TaxTreeGeneralizerActor<IN, OUT> withLevel(int level) {
    this.level = level;
    return this;
  }

  public TaxTreeGeneralizerActor<IN, OUT> withDTree(LeafLocator<IN> dTree) {
    this.dTree = dTree;
    return this;
  }

  public TaxTreeGeneralizerActor<IN, OUT> withResultParser(
      Function<TaxTreeNode, OUT> parser) {
    this.resultParser = parser;
    return this;
  }

  public TaxTreeGeneralizerActor<IN, OUT> withInputParser(
      Function<IN, OUT> parser) {
    this.inputPaser = parser;
    return this;
  }

  protected OUT parseNode(TaxTreeNode tnode) {
    Predictor.predict(this.resultParser != null, "result parser can't be null");
    return this.resultParser.apply(tnode);
  }

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

  public LeafLocator<IN> getdTree() {
    return dTree;
  }

  public void setdTree(LeafLocator<IN> dTree) {
    this.dTree = dTree;
  }
}
