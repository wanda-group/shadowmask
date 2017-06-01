package org.shadowmask.core.algorithms.ga;

public interface FitnessCalculator<FIT extends Fitness, IND extends Individual> {

  FIT calculate(IND ind);
}
