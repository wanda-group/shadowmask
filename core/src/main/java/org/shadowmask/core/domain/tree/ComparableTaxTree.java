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
package org.shadowmask.core.domain.tree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.javatuples.Pair;
import org.shadowmask.core.domain.TaxTreeType;
import org.shadowmask.core.domain.tree.ComparableTaxTree.ComparableTaxTreeNode;
import org.shadowmask.core.domain.treeobj.TreeObject;
import org.shadowmask.core.util.Predictor;

public abstract class ComparableTaxTree<T extends Comparable<T>>
    extends TaxTree<ComparableTaxTreeNode<T>> implements LeafLocator<T> {

  @Override protected ComparableTaxTreeNode<T> constructTNode(String jsonStr) {

    TreeObject<T> treeObject = constructTreeObject(jsonStr);
    ComparableTaxTreeNode<T> node = new ComparableTaxTreeNode<>();
    node.setName(treeObject.getText());
    node.setlBound(treeObject.getlBound());
    node.sethBound(treeObject.gethBound());
    return node;
  }

  @Override public TaxTreeType type() {
    return TaxTreeType.COMPARABLE;
  }

  // sort all the leave nodes
  @Override public void onTreeBuilt() {
    Collections
        .sort(this.getLeaves(), new Comparator<ComparableTaxTreeNode<T>>() {
          @Override public int compare(ComparableTaxTreeNode<T> o1,
              ComparableTaxTreeNode<T> o2) {
            return ComparableTaxTree.this.compare(o1, o2);
          }
        });
  }

  public void buildFromSortedValues(Iterator<T> sortedValuesIterator,
      int totCount, int bottomLevelSize, int[] levelScale) {

    Predictor.predict(sortedValuesIterator != null,
        "values iterator should not be null");
    Predictor.predict(bottomLevelSize > 0, "bottom size should > 0");

    Predictor.predict(totCount > bottomLevelSize,
        "total count should bigger than bottom level size");

    if (levelScale.length > 0) {
      int size = bottomLevelSize;
      for (int i = 0; i < levelScale.length; i++) {
        Predictor.predict(levelScale[i] < size && levelScale[i] > 0,
            "level scale should decrease gradually and must gt zero");
        size = levelScale[i];
      }
      if (levelScale[levelScale.length - 1] != 1) {
        int[] newScale = new int[levelScale.length + 1];
        int i;
        for (i = 0; i < levelScale.length; i++) {
          newScale[i] = levelScale[i];
        }
        newScale[i] = 1;
        levelScale = newScale;
      }
    } else {
      levelScale = new int[] { 1 };
    }
    Pair<T, T>[] bottomLHBounds = new Pair[bottomLevelSize];
    int step = totCount / bottomLevelSize;
    int i = 0;
    for (i = 0; i < bottomLevelSize; i++) {
      T l = sortedValuesIterator.next();
      T h = l;
      for (int j = 1; j < step; j++) {
        h = sortedValuesIterator.next();
      }
      bottomLHBounds[i] = new Pair<>(l, h);
    }
    // process tails
    --i;
    T h = bottomLHBounds[i].getValue1();
    while (sortedValuesIterator.hasNext()) {
      h = sortedValuesIterator.next();
    }
    bottomLHBounds[i] = new Pair<>(bottomLHBounds[i].getValue0(), h);

    // build leaves
    this.leaves = new ArrayList<>(bottomLevelSize);
    for (Pair<T, T> bound : bottomLHBounds) {
      ComparableTaxTreeNode<T> t = new ComparableTaxTreeNode<T>();
      t.setlBound(bound.getValue0());
      t.sethBound(bound.getValue1());
      t.setName(bound.getValue0() + "~" + bound.getValue1());
      this.leaves.add(t);
    }

    int nextLevelSize = bottomLevelSize;
    Iterator<ComparableTaxTreeNode<T>> nodeIterator = this.leaves.iterator();
    for (int scale : levelScale) {
      int mergeSize = nextLevelSize / scale;
      int parentLevelIdx;
      List<ComparableTaxTreeNode<T>> parentList = new ArrayList<>();
      for (parentLevelIdx = 0; parentLevelIdx < scale; parentLevelIdx++) {
        ComparableTaxTreeNode<T> parent = new ComparableTaxTreeNode<>();
        List<ComparableTaxTreeNode> children = new ArrayList<>();
        ComparableTaxTreeNode<T> childNode = nodeIterator.next();
        children.add(childNode);
        childNode.setParent(parent);
        T lB = childNode.lBound;
        T hB = childNode.hBound;
        parent.setChildren(children);
        int childIndex;
        for (childIndex = 1; childIndex < mergeSize; childIndex++) {
          childNode = nodeIterator.next();
          children.add(childNode);
          childNode.setParent(parent);
          hB = childNode.hBound;
        }
        if (parentLevelIdx == scale - 1) {
          while (nodeIterator.hasNext()) {
            childNode = nodeIterator.next();
            children.add(childNode);
            childNode.setParent(parent);
            hB = childNode.hBound;
          }
        }
        parent.setlBound(lB);
        parent.sethBound(hB);
        parent.setName(lB + "~" + hB);
        parentList.add(parent);
      }
      nextLevelSize = parentList.size();
      nodeIterator = parentList.iterator();
    }
    this.root = nodeIterator.next();
    this.height = levelScale.length + 1;
  }

  public int compare(ComparableTaxTreeNode<T> node1,
      ComparableTaxTreeNode<T> node2) {
    return node1.getlBound().compareTo(node2.getlBound());
  }

  /**
   * construct a tree node object
   */
  protected abstract TreeObject<T> constructTreeObject(String json);

  /**
   * binary search to find a leaf node
   */
  public ComparableTaxTreeNode<T> fixALeaf(T t) {
    int head = 0, tail = this.getLeaves().size(), originHead = head;
    ComparableTaxTreeNode<T> node = null;
    while (true) {
      originHead = head;
      int half = (head + tail) / 2;
      node = this.getLeaves().get(half);
      if (leafCompareValue(node, t) == 0) {
        return node;
      } else if (leafCompareValue(node, t) > 0) {
        tail = half;
      } else {
        head = half;
      }
      if (half == originHead) {
        break;
      }
    }
    return null;
  }

  @Override public ComparableTaxTreeNode<T> locate(T t) {
    return fixALeaf(t);
  }

  protected int leafCompareValue(ComparableTaxTreeNode<T> leaf, T value) {
    int comp1 = leaf.getlBound().compareTo(value);
    int comp2 = leaf.gethBound().compareTo(value);
    int mRes = comp1 * comp2;
    if (mRes <= 0) {
      return 0;
    } else {
      return (comp1 + comp2) < 0 ? -1 : 1;
    }
  }

  @Override public void onRelationBuilt(ComparableTaxTreeNode<T> parent,
      List<ComparableTaxTreeNode<T>> children) {
    T lBound = null;
    T hBound = null;

    for (ComparableTaxTreeNode<T> node : children) {
      if (lBound == null || node.getlBound().compareTo(lBound) < 0) {
        lBound = node.getlBound();
      }
      if (hBound == null || node.gethBound().compareTo(hBound) > 0) {
        hBound = node.gethBound();
      }
    }
    parent.sethBound(hBound);
    parent.setlBound(lBound);
  }

  public static class ComparableTaxTreeNode<T> extends TaxTreeNode {
    private T lBound;

    private T hBound;

    public T getlBound() {
      return lBound;
    }

    public void setlBound(T lBound) {
      this.lBound = lBound;
    }

    public T gethBound() {
      return hBound;
    }

    public void sethBound(T hBound) {
      this.hBound = hBound;
    }
  }

}
