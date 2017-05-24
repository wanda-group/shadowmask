package com.shadowmask.algorithms.ga.functions;

import com.shadowmask.algorithms.ga.Individual;

public interface MutationFunction<IND extends Individual> {

  IND mutate(IND ind);
}
