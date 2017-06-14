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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.shadowmask.core.domain.tree.TaxTreeNode;
import org.shadowmask.engine.spark.autosearch.pso.MkVelocity;

public class TaxTreeClusterMkVelocity extends MkVelocity
    implements Serializable {

  Dimension[] dimensions;
  int[] levelBounds;
  private int size;

  @Override public void init() {
    this.dimensions = new Dimension[size];
    for (int i = 0; i < this.size; i++) {
      Dimension d = new Dimension();
      d.setMasterDeltaLevel(
          new Random().nextInt(levelBounds[i]) * 2 - levelBounds[i]);
      d.setSlaveDeltaLevelMap(new HashMap<TaxTreeNode, Integer>());
      this.dimensions[i] = d;
    }
  }

  @Override public int dimension() {
    return this.getSize();
  }

  public Dimension[] getDimensions() {
    return dimensions;
  }

  public void setDimensions(Dimension[] dimensions) {
    this.dimensions = dimensions;
  }

  public void setLevelBounds(int[] levelBounds) {
    this.levelBounds = levelBounds;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  static class Dimension implements Serializable {
    private Integer masterDeltaLevel;

    private Map<TaxTreeNode, Integer> slaveDeltaLevelMap;

    public int getMasterDeltaLevel() {
      return masterDeltaLevel;
    }

    public void setMasterDeltaLevel(int masterDeltaLevel) {
      this.masterDeltaLevel = masterDeltaLevel;
    }

    public Map<TaxTreeNode, Integer> getSlaveDeltaLevelMap() {
      return slaveDeltaLevelMap;
    }

    public void setSlaveDeltaLevelMap(
        Map<TaxTreeNode, Integer> slaveDeltaLevelMap) {
      this.slaveDeltaLevelMap = slaveDeltaLevelMap;
    }
  }

}
