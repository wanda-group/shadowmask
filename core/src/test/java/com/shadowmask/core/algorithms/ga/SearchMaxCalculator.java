package com.shadowmask.core.algorithms.ga;

import com.shadowmask.core.algorithms.pso.PSOTTarget;
import java.math.BigDecimal;
import org.shadowmask.core.algorithms.ga.FitnessCalculator;

public class SearchMaxCalculator implements
    FitnessCalculator<SearchMaxFitness, SearchMaxIndividual> {

  static interface Function<T,R>{
    R apply(T t);
  }


  public Function<BigDecimal, Double> targetFunc = new Function<BigDecimal,Double>() {
    @Override public Double apply(BigDecimal i) {
      Double d  = i.doubleValue();
      return PSOTTarget.func(d);
    }
  };
//etFunc = i -> 7.8 * Math.pow(i, 7) + 2 * Math.pow(1, 4)
//      + 2 * Math.pow(1, 3) + 10;

  @Override
  public SearchMaxFitness calculate(SearchMaxIndividual searchMaxIndividual) {
    SearchMaxFitness fitness = new SearchMaxFitness();
    fitness.value = targetFunc.apply(searchMaxIndividual.chromosomes().get(0).xValue);
    return fitness;
  }
}
