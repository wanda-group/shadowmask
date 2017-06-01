package com.shadowmask.core.algorithms.ga;

public class MutationFunction implements org.shadowmask.core.algorithms.ga.functions.MutationFunction<SearchMaxIndividual> {

  @Override
  public SearchMaxIndividual mutate(SearchMaxIndividual searchMaxIndividual) {
    return searchMaxIndividual.mutate();
  }
}
