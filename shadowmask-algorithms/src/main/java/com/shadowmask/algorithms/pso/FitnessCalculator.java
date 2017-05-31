package com.shadowmask.algorithms.pso;

public interface FitnessCalculator<PA extends Particle, F extends Fitness> {
  public F fitness(PA pa);
}
