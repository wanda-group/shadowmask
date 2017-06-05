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

import org.shadowmask.core.algorithms.pso.ParticleAdaptor;
import org.shadowmask.engine.spark.ClassUtil;

public abstract class MkParticle
    extends ParticleAdaptor<MkPosition, MkVelocity, MkFitness> {

  private int dimension;

  @Override public void move(MkVelocity mkVelocity) {
    MkPosition position = this.currentPosition();
    // check null
    if (position == null) {
      throw new NullPointerException("pso particle's position cannot be null");
    }
    if (position.getGeneralizerActors() == null
        || position.getGeneralizerActors().length == 0) {
      throw new NullPointerException("generalizers cannot be null");
    }
    if (mkVelocity == null || mkVelocity.getVelocity().length == 0) {
      throw new NullPointerException("pso moving velocities cannot be null");
    }

    if (position.getGeneralizerActors().length != mkVelocity
        .getVelocity().length) {
      throw new RuntimeException(
          "dimension of generalizers and velocities should be equally");
    }
    position.move(mkVelocity);
  }

  @Override public void getBetter(MkPosition betterPosition,
      MkFitness betterFitness) {
    historyBestFitness = ClassUtil.clone(betterFitness);
    historyBestPosition = ClassUtil.clone(betterPosition);
  }




}
