package org.shadowmask.core.algorithms.ga;

public interface Fitness<F extends Fitness> {

  public boolean betterThan(F f);

}
