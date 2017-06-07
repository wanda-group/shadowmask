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

import java.util.Map.Entry;
import org.shadowmask.core.domain.tree.DomainTreeNode;
import org.shadowmask.core.mask.rules.generalizer.actor.DTreeGeneralizerActor;
import org.shadowmask.core.mask.rules.generalizer.actor.DtreeClusterGeneralizerActor;
import org.shadowmask.core.mask.rules.generalizer.actor.GeneralizerActor;
import org.shadowmask.core.util.ClassUtil;
import org.shadowmask.engine.spark.autosearch.pso.MkParticle;
import org.shadowmask.engine.spark.autosearch.pso.MkParticleDriver;
import org.shadowmask.engine.spark.autosearch.pso.MkVelocity;
import org.shadowmask.engine.spark.autosearch.pso.cluster.DtreeClusterMkVelocity.Dimension;

public class DtreeClusterMkParticleDriver implements MkParticleDriver {

  @Override public void drive(MkParticle pa, MkVelocity v) {
    DtreeClusterMkVelocity velocity = ClassUtil.cast(v);
    GeneralizerActor[] actors = pa.currentPosition().getGeneralizerActors();

    for (int i = 0; i < actors.length; i++) {
      DtreeClusterGeneralizerActor dtActor = ClassUtil.cast(actors[i]);
      Dimension dimension = velocity.getDimensions()[i];
      // update master
      DTreeGeneralizerActor masterActor =
          ClassUtil.<DTreeGeneralizerActor>cast(dtActor.getMasterGeneralizer())
              .updateLevel(dimension.getMasterDeltaLevel());

      for (Entry<DomainTreeNode, Integer> kv : dimension.getSlaveDeltaLevelMap()
          .entrySet()) {
        DTreeGeneralizerActor actor =
            ClassUtil.cast(dtActor.getSlaveMap().get(kv.getKey()));
        if (actor == null) {
          // generate a new special search node
          DTreeGeneralizerActor newActor = masterActor.newInstance();
          newActor.setdTree(masterActor.getdTree());
          newActor.setMaxLevel(masterActor.getMaxLevel());
          newActor.setMinLevel(masterActor.getMinLevel());
          newActor.setMaxLevel(kv.getValue());
          dtActor.getSlaveMap().put(kv.getKey(), newActor);
        } else {
          actor.updateLevel(kv.getValue());
        }
      }
    }

  }
}
