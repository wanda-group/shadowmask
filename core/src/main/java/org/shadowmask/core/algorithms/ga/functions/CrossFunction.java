package org.shadowmask.core.algorithms.ga.functions;

import org.shadowmask.core.algorithms.ga.Individual;
import org.javatuples.Pair;

public interface CrossFunction<IND extends Individual> {

  /**
   * cross operation
   */
  Pair<IND, IND> cross(IND ind1, IND ind2);
}
