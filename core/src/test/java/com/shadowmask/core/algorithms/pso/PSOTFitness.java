package com.shadowmask.core.algorithms.pso;

import org.shadowmask.core.algorithms.pso.Fitness;

public class PSOTFitness extends Fitness<PSOTFitness> {

  public double value;

  @Override public boolean betterThan(PSOTFitness psotFitness) {
    return this.compareTo(psotFitness) < 0;
  }

  @Override public int compareTo(PSOTFitness o) {
    return Double.valueOf(value).compareTo(Double.valueOf(o.value));
  }

  public static PSOTFitness valueOf(double d){
    PSOTFitness fitness = new PSOTFitness();
    fitness.value = d;
    return fitness;
  }

}
