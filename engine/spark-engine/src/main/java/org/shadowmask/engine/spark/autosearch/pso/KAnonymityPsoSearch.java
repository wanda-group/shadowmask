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
import java.util.Random;
import org.apache.spark.rdd.RDD;
import org.shadowmask.core.algorithms.pso.Swarm;
import org.shadowmask.core.mask.rules.generalizer.Generalizer;
import org.shadowmask.core.mask.rules.generalizer.GeneralizerActorAdaptor;
import org.shadowmask.engine.spark.Rethrow;
import org.shadowmask.engine.spark.autosearch.Executable;

/**
 * user pso search a solution of data mask
 */
public class KAnonymityPsoSearch
    extends Swarm<MkVelocity, MkFitness, MkPosition, MkParticle> {

  MkVelocityCalculator calculator = new MkVelocityCalculator() {
    @Override public double randomSearchRate() {
      return 0.01;
    }

    @Override public int randomVelocityDimension(int i) {
      return KAnonymityPsoSearch.this.dimension;
    }

    @Override public boolean isZERO(Double number) {
      return number * 1000000 != 0;
    }
  };

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
      final int index = i;
      // generate particles randomly
      MkParticle particle = new MkParticle() {
        @Override public MkPosition randomPosition() {
          MkPosition mkPosition = new MkPosition(dimension);
          for (int j = 0; j < dimension; ++j) {
            mkPosition.getGeneralizerActors()[j] =
                new GeneralizerActorAdaptor(generalizers[j],
                    new Random().nextInt(generalizers[j].getRootLevel() + 1));
          }
          return mkPosition;
        }

        @Override public MkVelocity randomVelocity() {
          MkVelocity mkVelocity = new MkVelocity(dimension);
          for (int j = 0; j < dimension; ++j) {
            mkVelocity.getVelocity()[j] =
                new Random().nextInt(generalizers[j].getRootLevel() + 1) * 2
                    - generalizers[j].getRootLevel();
          }
          return mkVelocity;
        }
      };
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
            .calculateFitness(particles(), fitnessMap, waitObject,privateTable);
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
