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
package org.shadowmask.core.algorithms.pso;

public abstract class ParticleAdaptor<P extends Position, V extends Velocity, F extends Fitness>
    implements Particle<P, V, F> {

  protected V currentVelocity;

  protected P currentPosition;

  protected F currentFitness;

  protected P historyBestPosition;

  protected F historyBestFitness;

  public ParticleAdaptor() {
    super();
    this.init();
    if (currentPosition() == null || currentPosition() == null) {
      throw new NullPointerException(
          "pso search position and velocity must not be null");
    }
  }

  @Override public void init() {
    currentPosition = randomPosition();
    currentVelocity = randomVelocity();
  }

  @Override public V currentVelocity() {
    return currentVelocity;
  }

  @Override public P currentPosition() {
    return currentPosition;
  }

  @Override public F currentFitness() {
    return currentFitness;
  }

  @Override public P historyBestPosition() {
    return historyBestPosition;
  }

  @Override public F historyBestFitness() {
    return historyBestFitness;
  }

  public abstract P randomPosition();

  public abstract V randomVelocity();
}
