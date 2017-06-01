package org.shadowmask.core.algorithms.ga.functions;

import org.shadowmask.core.algorithms.ga.Individual;

public interface MutationFunction<IND extends Individual> {

  IND mutate(IND ind);
}
