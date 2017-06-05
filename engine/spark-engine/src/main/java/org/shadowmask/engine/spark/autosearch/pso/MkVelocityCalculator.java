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

import java.util.Random;
import org.shadowmask.core.algorithms.pso.VelocityCalculator;

/**
 * calculate a new shadowmask pso Velocity
 */
public abstract class MkVelocityCalculator
    implements VelocityCalculator<MkVelocity, MkPosition, MkFitness> {

  public abstract double randomSearchRate();

  public abstract int randomVelocityDimension(int i);

  public abstract boolean isZERO(Double number);

  @Override public MkVelocity newVelocity(MkVelocity currentV,
      MkPosition currentPosition, MkFitness currentFitness,
      MkPosition historyBestPostion, MkFitness historyBestFitness,
      MkPosition globalBestPosition, MkFitness globalBestFitness,
      MkPosition currentBestPosition, MkFitness currentBestFitness,
      MkPosition currentWorstPosition, MkFitness currentWorstFitness) {

    int dimension = currentV.getVelocity().length;
    double rate = Math.random();
    MkVelocity velocity = new MkVelocity(dimension);
    if (rate < randomSearchRate()) {
      // random search
      for (int i = 0; i < dimension; ++i) {
        velocity.getVelocity()[i] = randomVelocityDimension(i);
      }
      return velocity;
    }

    for (int i = 0; i < dimension; ++i) {
      // try to get better
      double divider = currentBestFitness.fitnessValue() - currentWorstFitness
          .fitnessValue();
      // stay rate
      double stayRate = 0;
      if (!isZERO(divider)) {
        stayRate =
            (currentFitness.fitnessValue() - currentWorstFitness.fitnessValue())
                / divider;
      }

      divider =
          globalBestFitness.fitnessValue() - currentFitness.fitnessValue();
      // learn from self history best fitness
      double selfLearnRate = 1;
      if (!isZERO(divider)) {
        selfLearnRate =
            (historyBestFitness.fitnessValue() - currentFitness.fitnessValue())
                / divider;
      }
      // learn from global best particles
      double learnOther = 1 - selfLearnRate;
      double viD =
          currentV.getVelocity()[i] * stayRate + selfLearnRate * Math.random()
              * (historyBestPostion.getGeneralizerActors()[i].generalLevel()
              - currentPosition.getGeneralizerActors()[i].generalLevel())
              + learnOther * Math.random() * (
              globalBestPosition.getGeneralizerActors()[i].generalLevel()
                  - currentPosition.getGeneralizerActors()[i].generalLevel());
      int vi = (int) (new Random().nextInt(2) + viD);
      velocity.getVelocity()[i] = vi;
    }
    return velocity;
  }
}
