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

import java.util.Random;

public class CombineClusterMkVelocityCalculator
    extends ClusterMkVelocityCalculator {

  @Override protected Double learnedNewValue(Double currentVelocity,
      Double currentValue, Double historyBestValue, Double globalBestValue,
      LearnParameters p) {
    Double r = new Random().nextDouble();
    if (r < p.stayRate) {
      return 0D;
    } else {
      double lsFactor = p.lsRate + p.lsRandomScale;
      double loFactor = p.loRate + p.loRandomScale;
      if (isZERO(lsFactor) && isZERO(loFactor)) {
        r = new Random().nextDouble();
        if (r < 0.5) {
          return historyBestValue - currentValue;
        } else {
          return globalBestValue - currentValue;
        }
      } else {
        r = new Random().nextDouble();
        if (r < lsFactor / (lsFactor + loFactor)) {
          return historyBestValue - currentValue;
        } else {
          return globalBestValue - currentValue;
        }
      }

    }
  }

  @Override protected Double learnedNewValue(Integer currentVelocity,
      Integer currentValue, Integer historyBestValue, Integer globalBestValue,
      LearnParameters p) {
    return learnedNewValue(currentVelocity.doubleValue(),
        currentValue.doubleValue(), historyBestValue.doubleValue(),
        globalBestValue.doubleValue(), p);
  }

  @Override public double randomMutationRate() {
    return 0.5;
  }




}
