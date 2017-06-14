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

import java.io.Serializable;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.shadowmask.engine.spark.functions.DataAnoymizeFunction;

public class SparkDrivedDataAnoymizePSOSearch
    extends DataAnonymizePsoSearch<RDD<String>> implements Serializable {

  SparkDrivedFitnessCalculator calculator;
  private int threadNum;

  public SparkDrivedDataAnoymizePSOSearch(int threadNum) {
    this.threadNum = threadNum;
    calculator = new SparkDrivedFitnessCalculator(threadNum);
  }

  @Override protected MkFitnessCalculator<RDD<String>> mkFitnessCalculator() {
    return calculator;
  }

  public void setSc(SparkContext sc) {
    this.calculator.sc = sc;
  }

  public void setSeparator(Broadcast<String> separator) {
    this.calculator.separator = separator;
  }

  public void setDataGeneralizerIndex(
      Broadcast<Map<Integer, Integer>> dataGeneralizerIndex) {
    this.calculator.dataGeneralizerIndex = dataGeneralizerIndex;
  }

  public static class SparkDrivedFitnessCalculator
      extends MkFitnessCalculator<RDD<String>> implements Serializable {

    private SparkContext sc;

    private Broadcast<String> separator;

    private Broadcast<Map<Integer, Integer>> dataGeneralizerIndex;

    public SparkDrivedFitnessCalculator(int threadNums) {
      super(threadNums);
    }

    @Override public MkFitness calculateOne(MkParticle particle,
        RDD<String> dataSet) {

      RDD<String> anoymizedTable = DataAnoymizeFunction
          .anoymize(sc, dataSet, particle, separator, dataGeneralizerIndex);

      Object res = anoymizedTable.sample(false,0.001,System.currentTimeMillis()).collect();

      String [] res2 = (String[]) res;
      for (String s : res2) {
        System.out.println(s);
      }
//      System.out.println(res);
      return null;
    }


  }
}
