package com.shadowmask.algorithms.ga;

import java.math.BigDecimal;
import java.util.function.Function;

public class SearchMaxCalculator implements
    FitnessCalculator<SearchMaxFitness, SearchMaxIndividual> {

  public Function<Integer, BigDecimal> targetFunc = i -> BigDecimal.ZERO.subtract(BigDecimal.valueOf(i-10000).multiply(BigDecimal.valueOf(i-10000)));
//etFunc = i -> 7.8 * Math.pow(i, 7) + 2 * Math.pow(1, 4)
//      + 2 * Math.pow(1, 3) + 10;

  @Override
  public SearchMaxFitness calculate(SearchMaxIndividual searchMaxIndividual) {
    SearchMaxFitness fitness = new SearchMaxFitness();
    fitness.value = targetFunc.apply(searchMaxIndividual.chromosomes().get(0).xValue);
    return fitness;
  }
}
