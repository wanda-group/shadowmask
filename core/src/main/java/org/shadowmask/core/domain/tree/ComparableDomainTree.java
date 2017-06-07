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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.shadowmask.core.domain.tree.ComparableDomainTree.ComparableDomainTreeNode;
import org.shadowmask.core.domain.treeobj.TreeObject;

public abstract class ComparableDomainTree<T extends Comparable<T>>
    extends DomainTree<ComparableDomainTreeNode<T>>
    implements LeafLocator<T, ComparableDomainTreeNode<T>> {

  @Override protected ComparableDomainTreeNode<T> constructTNode(
      String jsonStr) {

    TreeObject<T> treeObject = constructTreeObject(jsonStr);
    ComparableDomainTreeNode<T> node = new ComparableDomainTreeNode<>();
    node.setName(treeObject.getText());
    node.setlBound(treeObject.getlBound());
    node.sethBound(treeObject.gethBound());
    return node;
  }

  // sort all the leave nodes
  @Override public void onTreeBuilt() {
    Collections
        .sort(this.getLeaves(), new Comparator<ComparableDomainTreeNode<T>>() {
          @Override public int compare(ComparableDomainTreeNode<T> o1,
              ComparableDomainTreeNode<T> o2) {
            return ComparableDomainTree.this.compare(o1, o2);
          }
        });
  }

  public int compare(ComparableDomainTreeNode<T> node1,
      ComparableDomainTreeNode<T> node2) {
    return node1.getlBound().compareTo(node2.getlBound());
  }

  /**
   * construct a tree node object
   */
  protected abstract TreeObject<T> constructTreeObject(String json);

  /**
   * binary search to find a leaf node
   */
  public ComparableDomainTreeNode<T> fixALeaf(T t) {
    int head = 0, tail = this.getLeaves().size(), originHead = head;
    ComparableDomainTreeNode<T> node = null;
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

  @Override public ComparableDomainTreeNode<T> locate(T t) {
    return fixALeaf(t);
  }

  protected int leafCompareValue(ComparableDomainTreeNode<T> leaf, T value) {
    int comp1 = leaf.getlBound().compareTo(value);
    int comp2 = leaf.gethBound().compareTo(value);
    int mRes = comp1 * comp2;
    if (mRes <= 0) {
      return 0;
    } else {
      return (comp1 + comp2) < 0 ? -1 : 1;
    }
  }

  @Override public void onRelationBuilt(ComparableDomainTreeNode<T> parent,
      List<ComparableDomainTreeNode<T>> children) {
    T lBound = null;
    T hBound = null;

    for (ComparableDomainTreeNode<T> node : children) {
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

  public static class ComparableDomainTreeNode<T>
      extends DomainTreeNode<ComparableDomainTreeNode> {
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
