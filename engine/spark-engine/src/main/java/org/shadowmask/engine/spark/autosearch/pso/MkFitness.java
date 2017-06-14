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

import org.shadowmask.core.algorithms.pso.Fitness;
import org.shadowmask.core.util.JsonUtil;

public class MkFitness extends Fitness<MkFitness> {

  private int k;

  private double fitnessValue;

  private double outlierSize;


  private double depthRate;

  private Double eLossRate;

  public void setDepthRate(double depthRate) {
    this.depthRate = depthRate;
  }

  private transient int ks[];
  private transient String kcs[];
  public void setKs(int[] ks) {
    this.ks = ks;
  }

  public void setLossRate(Double eLossRate) {
    this.eLossRate = eLossRate;
  }

  @Override public boolean betterThan(MkFitness that) {
//    return this.fitnessValue()*outlierSize < that.fitnessValue()*that.getOutlierSize();
    return this.fitnessValue() < that.fitnessValue();
  }

  public double getFitnessValue() {
    return fitnessValue;
  }

  public void setFitnessValue(double fitnessValue) {
    this.fitnessValue = fitnessValue;
  }

  public double getOutlierSize() {
    return outlierSize;
  }

  public void setOutlierSize(double outlierSize) {
    this.outlierSize = outlierSize;
  }

  public int getK() {
    return k;
  }

  public void setK(int k) {
    this.k = k;
  }

  public double fitnessValue() {
    return fitnessValue;
  }

  public void setKcs(String[] kcs) {
    this.kcs = kcs;
  }

  @Override public String toString() {
    return String.format("%s;%s;%s;%s",fitnessValue,outlierSize,eLossRate,depthRate);
  }
}
