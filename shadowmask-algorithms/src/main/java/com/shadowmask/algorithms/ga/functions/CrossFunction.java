package com.shadowmask.algorithms.ga.functions;

import com.shadowmask.algorithms.ga.Individual;
import org.javatuples.Pair;

public interface CrossFunction<IND extends Individual> {

  /**
   * cross operation
   */
  Pair<IND, IND> cross(IND ind1, IND ind2);
}
