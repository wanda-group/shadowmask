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
package org.shadowmask.engine.spark.autosearch.pso.cluster;

import java.util.Random;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.domain.TaxTreeType;
import org.shadowmask.core.domain.tree.LeafLocator;
import org.shadowmask.core.domain.tree.TaxTree;
import org.shadowmask.core.mask.rules.generalizer.actor.TaxTreeClusterGeneralizerActor;
import org.shadowmask.core.mask.rules.generalizer.actor.TaxTreeGeneralizerActor;
import org.shadowmask.engine.spark.autosearch.pso.MkPosition;

public class TaxTreeClusterMkPosition extends MkPosition {

  private TaxTree[] trees;
  private DataType[] dataTypes;

  @Override public void init() {
    this.generalizerActors = new TaxTreeClusterGeneralizerActor[this.dimension];
    for (int i = 0; i < this.dimension; i++) {
      TaxTree tree = trees[i];
//      DataType dataType = dataTypes[i];
      TaxTreeClusterGeneralizerActor actor =
          new TaxTreeClusterGeneralizerActor();

      TaxTreeGeneralizerActor innerActor = new TaxTreeGeneralizerActor();
      //      switch (dataType) {
      //      case INTEGER:
      //        actor = new TaxTreeClusterGeneralizerActor<Integer, String>();
      //        innerActor = new TaxTreeGeneralizerActor<String, String>();
      //        break;
      //      case DATE:
      //        actor = new TaxTreeClusterGeneralizerActor<Date, String>();
      //        innerActor = new TaxTreeGeneralizerActor<Date, String>();
      //        break;
      //      case DECIMAL:
      //        actor = new TaxTreeClusterGeneralizerActor<Double, String>();
      //        innerActor = new TaxTreeGeneralizerActor<Double, String>();
      //      }

      int maxLevel = tree.type() == TaxTreeType.COMPARABLE
          ? tree.getHeight()
          : tree.getHeight() - 1;
      actor.withMasterGeneralizer(innerActor.withMaxLevel(maxLevel)
          .withLevel(new Random().nextInt(maxLevel + 1)).withDTreeAsTax(tree))
          .withTree((LeafLocator) tree);

      this.generalizerActors[i] = actor;
    }
  }

  public void setTrees(TaxTree[] trees) {
    this.trees = trees;
  }
}
