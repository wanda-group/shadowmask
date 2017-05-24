package com.shadowmask.algorithms.ga;

import org.javatuples.Pair;

public interface Gene<G extends Gene> {

  public Pair<G, G> cross(G another);

  public G mutate();
}
