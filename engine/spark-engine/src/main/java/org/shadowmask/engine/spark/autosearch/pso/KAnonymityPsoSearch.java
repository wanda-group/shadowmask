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
import org.apache.spark.rdd.RDD;
import org.shadowmask.core.algorithms.pso.Swarm;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;
import org.shadowmask.engine.spark.Rethrow;
import org.shadowmask.engine.spark.autosearch.Executable;
import org.shadowmask.engine.spark.autosearch.pso.cluster.ClusterMkVelocityCalculator;
import org.shadowmask.engine.spark.autosearch.pso.cluster.DtreeClusterMkPosition;
import org.shadowmask.engine.spark.autosearch.pso.cluster.DtreeClusterMkVelocity;

/**
 * user pso search a solution of data mask
 */
public class KAnonymityPsoSearch
    extends Swarm<MkVelocity, MkFitness, MkPosition, MkParticle> {

  /**
   * dataset to be generalized .
   */
  private RDD<String> privateTable;
  /**
   * target k value .
   */
  private int k;
  private int maxSteps;
  private int particleSize;
  private int dimension;
  MkVelocityCalculator calculator = new ClusterMkVelocityCalculator();
  private List<MkParticle> particles = new ArrayList<>();

  private Generalizer[] generalizers;

  private MkFitnessCalculator mkFitnessCalculator = new MkFitnessCalculator(10);

  public KAnonymityPsoSearch() {

    try {
      init();
    } catch (ClassNotFoundException e) {
      Rethrow.rethrow(e);
    }
  }

  private void init() throws ClassNotFoundException {
    this.particles = new ArrayList<>();
    for (int i = 0; i < this.particleSize(); ++i) {
      // generate particles randomly
      MkParticle particle = new MkParticle() {
        @Override public MkPosition randomPosition() {
          MkPosition mkPosition = new DtreeClusterMkPosition();
          mkPosition.init();
          return mkPosition;
        }

        @Override public MkVelocity randomVelocity() {
          MkVelocity mkVelocity = new DtreeClusterMkVelocity();
          mkVelocity.init();
          return mkVelocity;
        }
      };
      particles.add(particle);
    }
  }

  @Override public List<MkParticle> particles() {
    return particles;

  }

  @Override public Map<MkParticle, MkFitness> calculateFitness() {
    final Object waitObject = new Object();
    final Map<MkParticle, MkFitness> fitnessMap = new HashMap<>();
    Executable executable = new Executable() {
      @Override public void exe() {
        mkFitnessCalculator
            .calculateFitness(particles(), fitnessMap, waitObject,
                privateTable);
      }
    };
    new Thread(executable).start();
    try {
      waitObject.wait();
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
}
