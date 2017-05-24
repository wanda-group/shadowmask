package com.shadowmask.algorithms.ga;

public class MutationFunction implements com.shadowmask.algorithms.ga.functions.MutationFunction<SearchMaxIndividual> {

  @Override
  public SearchMaxIndividual mutate(SearchMaxIndividual searchMaxIndividual) {
    return searchMaxIndividual.mutate();
  }
}
