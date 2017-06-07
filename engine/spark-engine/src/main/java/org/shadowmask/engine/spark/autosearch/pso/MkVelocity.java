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

import org.shadowmask.core.algorithms.pso.Velocity;

/**
 * pso velocity
 */
public class MkVelocity implements Velocity {

  /**
   * array of delta generalization level
   */
  private int[] velocity;

  public MkVelocity(int size) {
    this.velocity = new int[size];
  }

  public MkVelocity() {
  }

  public void init() {
  }

  public int[] getVelocity() {
    return velocity;
  }

  public void setVelocity(int[] velocity) {
    this.velocity = velocity;
  }
}
