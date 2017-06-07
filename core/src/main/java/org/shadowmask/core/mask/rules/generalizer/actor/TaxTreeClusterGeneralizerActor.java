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

import java.util.HashMap;
import java.util.Map;
import org.shadowmask.core.domain.tree.TaxTreeNode;
import org.shadowmask.core.domain.tree.LeafLocator;
import org.shadowmask.core.util.Predictor;

/**
 * find a generalizer from slaveMap ,if null use the master Generalizer
 */
public class TaxTreeClusterGeneralizerActor<IN, OUT>
    extends ClusterGeneralizerActor<IN, OUT> {

  private GeneralizerActor<IN, OUT> masterGeneralizer;

  private LeafLocator<IN> tree;

  private Map<TaxTreeNode, GeneralizerActor<IN, OUT>> slaveMap = new HashMap<>();

  @Override GeneralizerActor<IN, OUT> locateGeneralizer(IN in) {

    Predictor.predict(masterGeneralizer != null,
        "master generalizer should not be null");
    Predictor.predict(tree != null, "tree should not be null");
    Predictor.predict(slaveMap!=null, "slave map should not be null");

    TaxTreeNode leaf = tree.locate(in);
    GeneralizerActor<IN, OUT> generalizer = slaveMap.get(leaf);
    if (generalizer == null) {
      generalizer = masterGeneralizer;
    }
    return generalizer;
  }

  public TaxTreeClusterGeneralizerActor<IN, OUT> withTree(
      LeafLocator<IN> tree) {
    this.tree = tree;
    return this;
  }

  public TaxTreeClusterGeneralizerActor<IN, OUT> withMasterGeneralizer(
      GeneralizerActor<IN, OUT> masterGeneralizer) {
    this.masterGeneralizer = masterGeneralizer;
    return this;
  }

  public TaxTreeClusterGeneralizerActor<IN, OUT> addSlaveGeneralizer(
      TaxTreeNode tnode,GeneralizerActor<IN, OUT> generalizer) {
    Predictor.predict(slaveMap!=null, "slave map should not be null");
    this.slaveMap.put(tnode,generalizer);
    return this;
  }

  public GeneralizerActor<IN, OUT> getMasterGeneralizer() {
    return masterGeneralizer;
  }

  public LeafLocator<IN> getTree() {
    return tree;
  }

  public Map<TaxTreeNode, GeneralizerActor<IN, OUT>> getSlaveMap() {
    return slaveMap;
  }
}
