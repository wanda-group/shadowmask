package org.shadowmask.core.domain.tree;

/**
 * Comparable domain  node like age , salary etc.
 */
public class ComparableTaxNode<T extends Comparable<T>>
    extends TaxTreeNode implements Comparable<ComparableTaxNode<T>> {

  private T lowerBound;

  private T higherBound;

  @Override public int compareTo(ComparableTaxNode<T> o) {
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
