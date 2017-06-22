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
package org.shadowmask.engine.spark.autosearch.pso;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.shadowmask.core.algorithms.pso.Swarm;
import org.shadowmask.core.domain.tree.TaxTree;
import org.shadowmask.engine.spark.Rethrow;
import org.shadowmask.engine.spark.autosearch.Executable;
import org.shadowmask.engine.spark.autosearch.pso.cluster.LinearClusterMkVelocityCalculator;
import org.shadowmask.engine.spark.autosearch.pso.cluster.TaxTreeClusterMkPosition;
import org.shadowmask.engine.spark.autosearch.pso.cluster.TaxTreeClusterMkVelocity;

/**
 * user pso search a solution of data mask
 */
public abstract class DataAnonymizePsoSearch<TABLE>
    extends Swarm<MkVelocity, MkFitness, MkPosition, MkParticle> {

  MkVelocityCalculator calculator = new LinearClusterMkVelocityCalculator();

  /**
   * dataset to be generalized .
   */
  protected TABLE privateTable;
  private Long dataSize;
  private Double outlierRate;
  /**
   * target k value .
   */
  private int maxSteps = 10;
  private int particleSize = 2;
  private int dimension = 7;

  private TaxTree[] trees;
  private int[] treeHeights;
  private List<MkParticle> particles = new ArrayList<>();

  public void setCalculator(MkVelocityCalculator calculator) {
    this.calculator = calculator;
  }

  public void init() throws ClassNotFoundException {
    this.particles = new ArrayList<>();
    for (int i = 0; i < this.particleSize(); ++i) {
      // generate particles randomly
      MkParticle particle = new MkParticle() {
        @Override public MkPosition randomPosition() {
          TaxTreeClusterMkPosition mkPosition = new TaxTreeClusterMkPosition();
          mkPosition.setTrees(DataAnonymizePsoSearch.this.trees);
          mkPosition.setDimension(DataAnonymizePsoSearch.this.dimension);
          mkPosition.init();
          return mkPosition;
        }

        @Override public MkVelocity randomVelocity() {
          TaxTreeClusterMkVelocity mkVelocity = new TaxTreeClusterMkVelocity();
          mkVelocity.setSize(DataAnonymizePsoSearch.this.dimension);
          mkVelocity.setLevelBounds(DataAnonymizePsoSearch.this.treeHeights);
          mkVelocity.setTrees(trees);
          mkVelocity.init();
          return mkVelocity;
        }
      };
      particle.setParticleDriver(this.velocityDriver());
      particles.add(particle);
    }
  }

  protected abstract MkFitnessCalculator<TABLE> mkFitnessCalculator();

  protected abstract MkParticleDriver velocityDriver();

  @Override public List<MkParticle> particles() {
    return particles;

  }

  @Override public Map<MkParticle, MkFitness> calculateFitness() {
    final Object waitObject = new Object();
    final Map<MkParticle, MkFitness> fitnessMap = new HashMap<>();
    Executable executable = new Executable() {
      @Override public void exe() {
        mkFitnessCalculator()
            .calculateFitness(particles(), fitnessMap, waitObject,
                privateTable);
      }
    };

    new Thread(executable).start();
    try {
      synchronized (waitObject) {
        waitObject.wait();
      }
    } catch (InterruptedException e) {
      Rethrow.rethrow(e);
    }
    return fitnessMap;

  }

  @Override public Map<MkParticle, MkVelocity> calculateNewVelocities() {
    Map<MkParticle, MkVelocity> velocityMap = new HashMap<>();
    List<MkParticle> particles = particles();
    for (MkParticle particle : particles) {
      velocityMap.put(particle, this.calculator
          .newVelocity(particle.currentVelocity(), particle.currentPosition(),
              particle.currentFitness(), particle.historyBestPosition(),
              particle.historyBestFitness(),
              this.globalBestParticle().historyBestPosition(),
              this.globalBestParticle().historyBestFitness(),
              this.currentBestParticle().currentPosition(),
              this.currentBestParticle().currentFitness(),
              this.currentWorstParticle().currentPosition(),
              this.currentWorstParticle().currentFitness()));
    }
    return velocityMap;
  }

  @Override public int maxSteps() {
    return maxSteps;
  }

  @Override public int particleSize() {
    return particleSize;
  }

  public void setPrivateTable(TABLE privateTable) {
    this.privateTable = privateTable;
  }

  public void setTrees(TaxTree[] trees) {
    this.trees = trees;
  }

  public void setTreeHeights(int[] treeHeights) {
    this.treeHeights = treeHeights;
  }

  public Double getOutlierRate() {
    return outlierRate;
  }

  public void setOutlierRate(Double outlierRate) {
    this.outlierRate = outlierRate;
  }

  public Long getDataSize() {
    return dataSize;
  }

  public void setDataSize(Long dataSize) {
    this.dataSize = dataSize;
  }

  public void setMaxSteps(int maxSteps) {
    this.maxSteps = maxSteps;
  }

  public void setParticleSize(int particleSize) {
    this.particleSize = particleSize;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }
}
