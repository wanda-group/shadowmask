package org.shadowmask.core.algorithms.ga;

import org.javatuples.Pair;

public interface Chromosome<CH extends Chromosome> {

  /**
   * chromosome cross
   */
  public abstract Pair<CH, CH> cross(CH chromosome);

  /**
   * chromosome mutation
   */
  public abstract CH mutate();

}
