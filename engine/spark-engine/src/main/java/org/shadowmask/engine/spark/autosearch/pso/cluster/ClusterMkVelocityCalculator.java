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

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.shadowmask.core.domain.tree.TaxTree;
import org.shadowmask.core.domain.tree.TaxTreeNode;
import org.shadowmask.core.domain.tree.LeafLocator;
import org.shadowmask.core.mask.rules.generalizer.actor.TaxTreeGeneralizerActor;
import org.shadowmask.core.mask.rules.generalizer.actor.TaxTreeClusterGeneralizerActor;
import org.shadowmask.core.mask.rules.generalizer.actor.GeneralizerActor;
import org.shadowmask.core.util.ClassUtil;
import org.shadowmask.core.util.Predictor;
import org.shadowmask.engine.spark.autosearch.pso.MkPosition;
import org.shadowmask.engine.spark.autosearch.pso.MkVelocity;
import org.shadowmask.engine.spark.autosearch.pso.MkVelocityCalculator;
import org.shadowmask.engine.spark.autosearch.pso.cluster.TaxTreeClusterMkVelocity.Dimension;

public class ClusterMkVelocityCalculator extends MkVelocityCalculator {
  @Override public double randomSearchRate() {
    return 0.01;
  }

  @Override public boolean isZERO(Double number) {
    return number * 1000000 != 0;
  }

  @Override protected MkVelocity calcVelocity(Double stayRate, Double lsRate,
      Double lsRandomScale, Double loRate, Double loRandomScale,
      MkVelocity currentVelocity, MkPosition currentPosition,
      MkPosition historyBestPosition, MkPosition globalBestPosition) {

    Predictor.predict(currentVelocity instanceof TaxTreeClusterMkVelocity,
        "velocity type not match");
    Predictor.predict(historyBestPosition instanceof TaxTreeClusterMkPosition,
        "type not math");
    Predictor.predict(globalBestPosition instanceof TaxTreeClusterMkPosition,
        "type not math");

    TaxTreeClusterMkVelocity currentV = ClassUtil.cast(currentVelocity);
    TaxTreeClusterMkPosition hbPosition = ClassUtil.cast(historyBestPosition);
    TaxTreeClusterMkPosition gbPosition = ClassUtil.cast(globalBestPosition);
    TaxTreeClusterMkPosition curPosition = ClassUtil.cast(currentPosition);

    // learn parameters
    LearnParameters param =
        new LearnParameters(stayRate, lsRate, lsRandomScale, loRate,
            loRandomScale);

    Predictor.predict(currentV.getDimensions().length == hbPosition
        .getGeneralizerActors().length
        && hbPosition.getGeneralizerActors().length == gbPosition
        .getGeneralizerActors().length
        && gbPosition.getGeneralizerActors().length == curPosition
        .getGeneralizerActors().length, "Dimension not match");

    TaxTreeClusterMkVelocity velocity = new TaxTreeClusterMkVelocity();
    int d = currentV.getVelocity().length;
    velocity.setDimensions(new Dimension[d]);

    for (int i = 0; i < d; i++) {
      velocity.getDimensions()[i] = newDimension(currentV.getDimensions()[i],
          ClassUtil.<TaxTreeClusterGeneralizerActor>cast(
              curPosition.getGeneralizerActors()[i]),
          ClassUtil.<TaxTreeClusterGeneralizerActor>cast(
              hbPosition.getGeneralizerActors()[i]),
          ClassUtil.<TaxTreeClusterGeneralizerActor>cast(
              gbPosition.getGeneralizerActors()[i]), param);
    }
    return velocity;
  }

  private Dimension newDimension(Dimension curD,
      TaxTreeClusterGeneralizerActor curActor,
      TaxTreeClusterGeneralizerActor hbActor,
      TaxTreeClusterGeneralizerActor gbActor, LearnParameters learnParam) {
    Dimension dimension = new Dimension();

    // learned new master velocity
    TaxTreeGeneralizerActor curMaster =
        ClassUtil.cast(curActor.getMasterGeneralizer());
    TaxTreeGeneralizerActor hbMaster =
        ClassUtil.cast(hbActor.getMasterGeneralizer());
    TaxTreeGeneralizerActor gbMaster =
        ClassUtil.cast(gbActor.getMasterGeneralizer());

    int masterDV = Double.valueOf(
        learnedNewValue(curD.getMasterDeltaLevel(), curMaster.getLevel(),
            hbMaster.getLevel(), gbMaster.getLevel(), learnParam) + 0.5d)
        .intValue();

    dimension.setMasterDeltaLevel(masterDV);

    // learned from new slave velocity
    Set<TaxTreeNode> specialNodes = new HashSet<>();
    specialNodes.addAll(curActor.getSlaveMap().keySet());
    specialNodes.addAll(hbActor.getSlaveMap().keySet());
    specialNodes.addAll(hbActor.getSlaveMap().keySet());

    for (TaxTreeNode node : specialNodes) {
      Integer curV = curD.getSlaveDeltaLevelMap().get(node);
      TaxTreeGeneralizerActor aCurActor =
          ClassUtil.cast(curActor.getSlaveMap().get(node));
      TaxTreeGeneralizerActor aHbActor =
          ClassUtil.cast(hbActor.getSlaveMap().get(node));
      TaxTreeGeneralizerActor aGbActor =
          ClassUtil.cast(gbActor.getSlaveMap().get(node));
      if (curV == null && aCurActor == null && aHbActor == null
          && aGbActor == null) {
        continue;
      } else {
        if (curV == null) {
          curV = ClassUtil.<TaxTreeGeneralizerActor>cast(
              curActor.getMasterGeneralizer()).getLevel();
        }
        if (aCurActor == null) {
          aCurActor = curMaster;
        }
        if (aHbActor == null) {
          aHbActor = hbMaster;
        }
        if (aGbActor == null) {
          aGbActor = gbMaster;
        }
        int vd = Double.valueOf(
            learnedNewValue(curV, aCurActor.getLevel(), aHbActor.getLevel(),
                aGbActor.getLevel(), learnParam) + 0.5D).intValue();
        dimension.getSlaveDeltaLevelMap().put(node, vd);
      }
    }

    //    curActor.getSlaveMap().entrySet().fo

    return dimension;
  }

  private Double learnedNewValue(Double currentVelocity, Double currentValue,
      Double historyBestValue, Double globalBestValue, LearnParameters p) {
    return p.stayRate * currentVelocity + p.lsRate * p.lsRandomScale * (
        historyBestValue - currentValue) + p.loRate * p.loRandomScale * (
        globalBestValue - currentValue);
  }

  private Double learnedNewValue(Integer currentVelocity, Integer currentValue,
      Integer historyBestValue, Integer globalBestValue, LearnParameters p) {
    return p.stayRate * currentVelocity + p.lsRate * p.lsRandomScale * (
        historyBestValue - currentValue) + p.loRate * p.loRandomScale * (
        globalBestValue - currentValue);
  }

  /**
   * generate a velocity randomly
   *
   * @param currentPosition
   * @return
   */
  @Override protected MkVelocity randomVelocity(MkPosition currentPosition) {
    Predictor.predict(currentPosition instanceof TaxTreeClusterMkPosition,
        "type not math");

    TaxTreeClusterMkPosition curPos = (TaxTreeClusterMkPosition) currentPosition;
    GeneralizerActor[] actors = curPos.getGeneralizerActors();
    Predictor.predict(actors != null && actors.length > 0,
        "generalizer actors should be null or empty array");

    TaxTreeClusterMkVelocity velocity = new TaxTreeClusterMkVelocity();
    TaxTreeClusterMkVelocity.Dimension[] dimensions =
        new Dimension[actors.length];
    for (int i = 0; i < actors.length; i++) {
      dimensions[i] = randomDimension(
          ClassUtil.<TaxTreeClusterGeneralizerActor>cast(actors[i]));
    }
    return velocity;
  }

  private Dimension randomDimension(TaxTreeClusterGeneralizerActor actor) {
    Dimension dimension = new Dimension();
    // random search master level
    GeneralizerActor generalizer = actor.getMasterGeneralizer();
    Predictor.predict(generalizer instanceof TaxTreeGeneralizerActor,
        "actor type not match");

    TaxTreeGeneralizerActor masterActor = (TaxTreeGeneralizerActor) generalizer;

    int randomMasterLevel = new Random()
        .nextInt(masterActor.getMaxLevel() + 1 - masterActor.getMinLevel())
        - masterActor.getLevel();

    dimension.setMasterDeltaLevel(randomMasterLevel);

    // slave actors
    Map<TaxTreeNode, GeneralizerActor> slaveMap = actor.getSlaveMap();
    for (Entry<TaxTreeNode, GeneralizerActor> kv : slaveMap.entrySet()) {
      Predictor.predict(kv.getValue() instanceof TaxTreeGeneralizerActor,
          "actor type not match");
      TaxTreeGeneralizerActor slaveActor = (TaxTreeGeneralizerActor) kv.getValue();
      randomMasterLevel = new Random()
          .nextInt(slaveActor.getMaxLevel() + 1 - slaveActor.getMinLevel())
          - slaveActor.getLevel();
      dimension.getSlaveDeltaLevelMap().put(kv.getKey(), randomMasterLevel);
    }

    // random select dimension
    LeafLocator tree = actor.getTree();
    Predictor.predict(tree instanceof TaxTree, "domain tree not match");
    TaxTree dTree = (TaxTree) tree;
    int index = new Random().nextInt(dTree.getLeaves().size());

    TaxTreeNode node = (TaxTreeNode) dTree.getLeaves().get(index);
    int velocity = new Random()
        .nextInt(masterActor.getMaxLevel() + 1 - masterActor.getMinLevel())
        - masterActor.getLevel();
    if (velocity != masterActor.getLevel()) {
      dimension.getSlaveDeltaLevelMap().put(node, velocity);
    }
    return dimension;
  }

  class LearnParameters {
    public Double stayRate;
    public Double lsRate;
    public Double lsRandomScale;

    public Double loRate;

    public Double loRandomScale;

    public LearnParameters(Double stayRate, Double lsRate, Double lsRandomScale,
        Double loRate, Double loRandomScale) {
      this.stayRate = stayRate;
      this.lsRate = lsRate;
      this.lsRandomScale = lsRandomScale;
      this.loRate = loRate;
      this.loRandomScale = loRandomScale;
    }
  }
}
