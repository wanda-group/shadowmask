package org.shadowmask.core.algorithms.pso;

public abstract class Fitness<T extends Fitness<T>> implements Comparable<T> {
  public abstract boolean betterThan(T t);

  @Override public int compareTo(T o) {
    return this.compareTo(o);
  }
}
