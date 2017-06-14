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

import com.google.gson.Gson;
import java.util.Iterator;
import java.util.List;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.domain.treeobj.StringTreeObject;
import org.shadowmask.core.domain.treeobj.TreeObject;
import org.shadowmask.core.util.JsonUtil;

public class OrderedStringTaxTree extends ComparableTaxTree<String> {
  public static StrictComparator STRICT_COMPARE = new StrictComparator();
  public static PrefixComparator PREFIX_COMPARE = new PrefixComparator();
  public static CombineFromRootWithSeparatorComparator ROOT_COMBINE_COMPARE =
      new CombineFromRootWithSeparatorComparator();
  private StringNodeComparator comparator = STRICT_COMPARE;

  @Override protected TreeObject<String> constructTreeObject(String json) {
    Gson gson = JsonUtil.newGsonInstance();
    StringTreeObject obj = gson.fromJson(json, StringTreeObject.class);
    return obj;
  }

  @Override protected int leafCompareValue(ComparableTaxTreeNode<String> leaf,
      String value) {
    return comparator.compare(leaf, value);
  }

  public OrderedStringTaxTree withComparator(StringNodeComparator comparator) {
    this.comparator = comparator;
    return this;
  }

  @Override public void buildFromSortedValues(
      Iterator<String> sortedValuesIterator, int totCount, int bottomLevelSize,
      int[] levelScale) {
    throw new RuntimeException(
        "not support buildFromSortedValues in ordered String DTree ");
  }

  @Override public DataType dataType() {
    return DataType.COMPARABLE_STRING;
  }

  @Override public void onRelationBuilt(ComparableTaxTreeNode<String> parent,
      List<ComparableTaxTreeNode<String>> children) {

  }

  @Override public int compare(ComparableTaxTreeNode<String> node1,
      ComparableTaxTreeNode<String> node2) {
    return this.comparator.compare(node1, node2);
  }

  static interface StringNodeComparator {
    int compare(ComparableTaxTreeNode<String> leaf, String value);

    int compare(ComparableTaxTreeNode<String> leaf,
        ComparableTaxTreeNode<String> leaf1);
  }

  public static class StrictComparator implements StringNodeComparator {
    @Override public int compare(ComparableTaxTreeNode<String> leaf,
        String value) {
      return leaf.getName().compareTo(value);
    }

    @Override public int compare(ComparableTaxTreeNode<String> leaf,
        ComparableTaxTreeNode<String> leaf1) {
      return this.compare(leaf, leaf1.getName());
    }
  }

  public static class PrefixComparator implements StringNodeComparator {
    @Override public int compare(ComparableTaxTreeNode<String> leaf,
        String value) {
      int len = leaf.getName().length() < value.length()
          ? leaf.getName().length()
          : value.length();
      return leaf.getName().substring(0, len)
          .compareTo(value.substring(0, len));
    }

    @Override public int compare(ComparableTaxTreeNode<String> leaf,
        ComparableTaxTreeNode<String> leaf1) {
      return this.compare(leaf, leaf1.getName());
    }
  }

  public static class CombineFromRootWithSeparatorComparator
      implements StringNodeComparator {

    private String separator = ",";

    public static CombineFromRootWithSeparatorComparator newInstance() {
      return new CombineFromRootWithSeparatorComparator();
    }

    @Override public int compare(ComparableTaxTreeNode<String> leaf,
        String value) {
      String res = fullValue(leaf);
      if (res.length() > value.length()) {
        return 1;
      }
      int len = res.length();
      return res.substring(0, len).compareTo(value.substring(0, len));
    }

    @Override public int compare(ComparableTaxTreeNode<String> leaf,
        ComparableTaxTreeNode<String> leaf1) {
      return fullValue(leaf).compareTo(fullValue(leaf1));
    }

    private String fullValue(ComparableTaxTreeNode<String> leaf) {
      String res = "";
      ComparableTaxTreeNode<String> pointer = leaf;
      while (pointer.getParent() != null) {
        res = separator + pointer.getName() + res;
        pointer = (ComparableTaxTreeNode<String>) pointer.getParent();
      }
      res = pointer.getName() + res;
      return res;
    }

    public CombineFromRootWithSeparatorComparator withSeparator(
        String separator) {
      this.separator = separator;
      return this;
    }

  }

}
