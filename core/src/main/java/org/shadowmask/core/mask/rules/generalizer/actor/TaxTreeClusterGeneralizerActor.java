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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.shadowmask.core.domain.tree.DateTaxTree;
import org.shadowmask.core.domain.tree.LeafLocator;
import org.shadowmask.core.domain.tree.TaxTree;
import org.shadowmask.core.domain.tree.TaxTreeNode;
import org.shadowmask.core.util.Predictor;
import org.shadowmask.core.util.Rethrow;

/**
 * find a generalizer from slaveMap ,if null use the master Generalizer
 */
public class TaxTreeClusterGeneralizerActor
    extends ClusterGeneralizerActor<Object, String> {

  private GeneralizerActor masterGeneralizer;

  private LeafLocator tree;

  private int currentMaskLevel;

  private int maxLevel;

  private Map<TaxTreeNode, GeneralizerActor> slaveMap = new HashMap<>();

  @Override public String generalize(Object s) {
    return super.generalize(convertInput(s));
  }

  @Override GeneralizerActor locateGeneralizer(Object in) {

    Predictor.predict(masterGeneralizer != null,
        "master generalizer should not be null");
    Predictor.predict(tree != null, "tree should not be null");
    Predictor.predict(slaveMap != null, "slave map should not be null");

    TaxTree taxTree = (TaxTree) tree;

    TaxTreeNode leaf = tree.locate(convertInput(in));

    GeneralizerActor generalizer = slaveMap.get(leaf);
    if (generalizer == null) {
      generalizer = masterGeneralizer;
    }
    if(generalizer instanceof TaxTreeGeneralizerActor){
      this.currentMaskLevel = ((TaxTreeGeneralizerActor)generalizer).getLevel();
    }
    return generalizer;
  }

  public Object convertInput(Object in) {
    TaxTree taxTree = (TaxTree) tree;
    Object res = null;
    switch (taxTree.dataType()) {
    case INTEGER:
      res = Integer.valueOf(in.toString());
      break;
    case DECIMAL:
      res = Double.valueOf(in.toString());
      break;
    case DATE:
      DateTaxTree dateTaxTree = (DateTaxTree) taxTree;
      SimpleDateFormat formater =
          new SimpleDateFormat(dateTaxTree.getPattern());
      try {
        res = formater.parse(in.toString());
      } catch (ParseException e) {
        Rethrow.rethrow(e);
      }
      break;
    default:
      res = in;
    }
    return res;
  }

  public TaxTreeClusterGeneralizerActor withTree(LeafLocator tree) {
    this.tree = tree;
    return this;
  }

  public TaxTreeClusterGeneralizerActor withMasterGeneralizer(
      GeneralizerActor masterGeneralizer) {
    this.masterGeneralizer = masterGeneralizer;
    if(masterGeneralizer instanceof TaxTreeGeneralizerActor){
      this.maxLevel = ((TaxTreeGeneralizerActor)masterGeneralizer).getMaxLevel();
    }
    return this;
  }

  public TaxTreeClusterGeneralizerActor addSlaveGeneralizer(TaxTreeNode tnode,
      GeneralizerActor generalizer) {
    Predictor.predict(slaveMap != null, "slave map should not be null");
    this.slaveMap.put(tnode, generalizer);
    return this;
  }

  public GeneralizerActor getMasterGeneralizer() {
    return masterGeneralizer;
  }

  public LeafLocator getTree() {
    return tree;
  }

  public Map<TaxTreeNode, GeneralizerActor> getSlaveMap() {
    return slaveMap;
  }

  @Override public String toString() {
    String show = "";
    show+=this.masterGeneralizer.toString();
    for (GeneralizerActor actor : this.getSlaveMap().values()) {
      show+=actor.toString();
    }
    return show;
  }

  public int getCurrentMaskLevel() {
    return currentMaskLevel;
  }

  public int getMaxLevel() {
    return maxLevel;
  }
}
