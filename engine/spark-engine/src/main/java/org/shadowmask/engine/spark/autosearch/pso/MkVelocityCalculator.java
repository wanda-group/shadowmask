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

import org.javatuples.Quintet;
import org.shadowmask.core.algorithms.pso.VelocityCalculator;

/**
 * calculate a new shadowmask pso Velocity
 */
public abstract class MkVelocityCalculator
    implements VelocityCalculator<MkVelocity, MkPosition, MkFitness> {

  public abstract double randomSearchRate();

  public boolean isZERO(Double number) {
    return number * 1000000 == 0;
  }

  @Override public MkVelocity newVelocity(MkVelocity currentV,
      MkPosition currentPosition, MkFitness currentFitness,
      MkPosition historyBestPostion, MkFitness historyBestFitness,
      MkPosition globalBestPosition, MkFitness globalBestFitness,
      MkPosition currentBestPosition, MkFitness currentBestFitness,
      MkPosition currentWorstPosition, MkFitness currentWorstFitness) {

    double rate = Math.random();

    if (rate < randomSearchRate()) {
      // random search
      return this.randomVelocity(currentPosition);
    }

    Quintet<Double, Double, Double, Double, Double> params =
        this.learnParams(currentFitness, currentBestFitness,
            currentWorstFitness, historyBestFitness, globalBestFitness);

    MkVelocity velocity =
        this.calcVelocity(params.getValue0(), params.getValue1(),
            params.getValue2(), params.getValue3(), params.getValue4(),
            currentV, currentPosition, historyBestPostion, globalBestPosition);
    return velocity;
  }

  /**
   * calculate a new velocity
   *
   * @param stayRate            keep origin velocity
   * @param lsRate              learn from particle history best position
   * @param loRate              learn from best particle in the swarm
   * @param currentVelocity     current velocity of the paticle
   * @param historyBestPosition history best position of the particle
   * @param globalBestPosition  global best position in the swarm
   * @return a new velocity
   */
  protected abstract MkVelocity calcVelocity(Double stayRate, Double lsRate,
      Double lsRandomScale, Double loRate, Double loRandomScale,
      MkVelocity currentVelocity, MkPosition currentPosition,
      MkPosition historyBestPosition, MkPosition globalBestPosition);

  /**
   * generate a velocity randomly
   *
   * @return a new velocity
   */
  protected abstract MkVelocity randomVelocity(MkPosition currentPosition);

  /**
   * calculate learn parameters
   *
   * @param currentFitness
   * @param currentBestFitness
   * @param currentWorstFitness
   * @param historyBestFitness
   * @param globalBestFitness
   * @return stay rate , learn from self rate ,learn from self random scale ,learn from global best rate ,learn from best random scale
   */

  protected Quintet<Double, Double, Double, Double, Double> learnParams(
      MkFitness currentFitness, MkFitness currentBestFitness,
      MkFitness currentWorstFitness, MkFitness historyBestFitness,
      MkFitness globalBestFitness) {
    // try to get better
    double divider =
        currentBestFitness.fitnessValue() - currentWorstFitness.fitnessValue();
    // stay rate
    double stayRate = 0;
    if (!isZERO(divider)) {
      stayRate =
          (currentFitness.fitnessValue() - currentWorstFitness.fitnessValue())
              / divider;
    }

    divider = globalBestFitness.fitnessValue() - currentFitness.fitnessValue();
    // learn from self history best fitness
    double selfLearnRate = 1;
    if (!isZERO(divider)) {
      selfLearnRate =
          (historyBestFitness.fitnessValue() - currentFitness.fitnessValue())
              / divider;
    }
    // learn from global best particles
    double learnOther = 1 - selfLearnRate;
    return new Quintet<>(stayRate, selfLearnRate, Math.random(), learnOther,
        Math.random());
  }

  protected class LearnParameters {
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
