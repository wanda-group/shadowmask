package com.shadowmask.algorithms.pso;

public class PSOTFitness extends Fitness<PSOTFitness> {

  public double value;

  @Override public boolean betterThan(PSOTFitness psotFitness) {
    return this.compareTo(psotFitness) < 0;
  }

  @Override public int compareTo(PSOTFitness o) {
    return Double.valueOf(value).compareTo(Double.valueOf(o.value));
  }
}
