package org.shadowmask.core.algorithms.pso;

import java.io.Serializable;

public abstract class Fitness<T extends Fitness<T>>
    implements Comparable<T>, Serializable {
  public abstract boolean betterThan(T t);

  @Override public int compareTo(T o) {
    return this.compareTo(o);
  }
}
