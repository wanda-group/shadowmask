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

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ParticleAdaptor<P extends Position, V extends Velocity, F extends Fitness>
    implements Particle<P, V, F> {

  private static AtomicInteger idGenerator = new AtomicInteger(0);
  protected V currentVelocity;
  protected P currentPosition;
  protected F currentFitness;
  protected P historyBestPosition;
  protected F historyBestFitness;
  int id;

  public ParticleAdaptor() {
    super();
    this.init();
    id = idGenerator.incrementAndGet();
    if (currentPosition() == null || currentPosition() == null) {
      throw new NullPointerException(
          "pso search position and velocity must not be null");
    }
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ParticleAdaptor<?, ?, ?> that = (ParticleAdaptor<?, ?, ?>) o;

    return id == that.id;
  }

  @Override public int hashCode() {
    return id;
  }

  @Override public void updateVelocity(V velocity) {
    this.currentVelocity = velocity;
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

  public void setCurrentVelocity(V currentVelocity) {
    this.currentVelocity = currentVelocity;
  }

  public void setCurrentPosition(P currentPosition) {
    this.currentPosition = currentPosition;
  }

  public void setCurrentFitness(F currentFitness) {
    this.currentFitness = currentFitness;
  }

  public void setHistoryBestPosition(P historyBestPosition) {
    this.historyBestPosition = historyBestPosition;
  }

  public void setHistoryBestFitness(F historyBestFitness) {
    this.historyBestFitness = historyBestFitness;
  }
}
