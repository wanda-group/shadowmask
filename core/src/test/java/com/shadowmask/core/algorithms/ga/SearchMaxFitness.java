package com.shadowmask.core.algorithms.ga;

import java.math.BigDecimal;
import org.shadowmask.core.algorithms.ga.Fitness;

public class SearchMaxFitness implements Fitness<SearchMaxFitness> {

  public Double value;


  @Override public boolean betterThan(SearchMaxFitness searchMaxFitness) {
    return value < searchMaxFitness.value;
  }
}
