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
package com.shadowmask.core.domain;

import org.junit.Assert;
import org.junit.Test;
import org.shadowmask.core.domain.tree.ComparableDomainTree.ComparableDomainTreeNode;
import org.shadowmask.core.domain.tree.OrderedStringDomainTree;

public class TestOrderedStringTree {
  @Test public void test() {
    OrderedStringDomainTree tree = new OrderedStringDomainTree().withComparator(OrderedStringDomainTree.ROOT_COMBINE_COMPARE);
    tree.constructFromYamlInputStream(this.getClass().getClassLoader()
        .getResourceAsStream("Interval-String-Mask.yaml"));
    ComparableDomainTreeNode<String> node = tree.fixALeaf("中国,上海,浦东新区,浦东南路455号");
//    node = tree.fixALeaf("中国,上海,闵行");
    Assert.assertNotNull(node);
  }
}
