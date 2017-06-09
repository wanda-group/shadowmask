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
package com.shadowmask.core.mask.rules.generalization.actor;

import org.junit.Assert;
import org.junit.Test;
import org.shadowmask.core.domain.tree.DomainTreeNode;
import org.shadowmask.core.domain.tree.LeafLocator;
import org.shadowmask.core.domain.tree.OrderedStringDomainTree;
import org.shadowmask.core.mask.rules.generalizer.actor.DTreeGeneralizerActor;
import org.shadowmask.core.mask.rules.generalizer.actor.DtreeClusterGeneralizerActor;

public class TestDTreeGeneralizer {

  @Test public void test() {
    OrderedStringDomainTree tree = new OrderedStringDomainTree()
        .withComparator(OrderedStringDomainTree.ROOT_COMBINE_COMPARE);
    tree.constructFromYamlInputStream(this.getClass().getClassLoader()
        .getResourceAsStream("Interval-String-Mask.yaml"));

    DTreeGeneralizerActor<String, String> treeActor =
        new DTreeGeneralizerActor<String, String>() {

          @Override public DTreeGeneralizerActor<String, String> newInstance() {
            return null;
          }

          @Override protected String parseNode(DomainTreeNode tnode) {
            return tnode.getName();
          }
        }.withDTree((LeafLocator) tree).withLevel(0);

    DTreeGeneralizerActor<String, String> treeActor1 =
        new DTreeGeneralizerActor<String, String>() {

          @Override public DTreeGeneralizerActor<String, String> newInstance() {
            return null;
          }

          @Override protected String parseNode(DomainTreeNode tnode) {
            return tnode.getName();
          }
        }.withDTree((LeafLocator) tree).withLevel(2);

    DtreeClusterGeneralizerActor<String, String> clusterActor =
        new DtreeClusterGeneralizerActor<String, String>()
            .withMasterGeneralizer(treeActor).withTree((LeafLocator) tree)
            .addSlaveGeneralizer(tree.getLeaves().get(0),treeActor1);

    String res = clusterActor.generalize("中国,上海,浦东新区,浦东南路455号");
    Assert.assertEquals(res,"浦东南路");
    res = clusterActor.generalize("中国,上海,浦东新区,塘桥,xx路xx号");
    Assert.assertEquals(res,"上海");
  }
}
