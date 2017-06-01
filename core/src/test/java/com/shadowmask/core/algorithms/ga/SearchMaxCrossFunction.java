package com.shadowmask.core.algorithms.ga;

import org.shadowmask.core.algorithms.ga.functions.CrossFunction;
import org.javatuples.Pair;

public class SearchMaxCrossFunction implements CrossFunction<SearchMaxIndividual> {


  @Override
  public Pair<SearchMaxIndividual, SearchMaxIndividual> cross(SearchMaxIndividual ind1,
      SearchMaxIndividual ind2) {
    return ind1.cross(ind2);
  }
}
