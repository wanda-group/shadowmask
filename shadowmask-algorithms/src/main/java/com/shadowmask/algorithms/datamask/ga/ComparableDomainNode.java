package com.shadowmask.algorithms.datamask.ga;

/**
 * Comparable domain  node like age , salary etc.
 */
public class ComparableDomainNode<T extends Comparable<T>>
    extends DomainTreeNode implements Comparable<ComparableDomainNode<T>> {

  private T lowerBound;

  private T higherBound;

  @Override public int compareTo(ComparableDomainNode<T> o) {
    return this.getLowerBound().compareTo(o.getLowerBound());
  }

  public T getHigherBound() {
    return higherBound;
  }

  public void setHigherBound(T higherBound) {
    this.higherBound = higherBound;
  }

  public T getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(T lowerBound) {
    this.lowerBound = lowerBound;
  }
}
