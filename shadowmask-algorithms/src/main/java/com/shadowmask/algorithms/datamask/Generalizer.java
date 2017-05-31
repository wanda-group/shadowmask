package com.shadowmask.algorithms.datamask;

public interface Generalizer<T,R> {
  public R generalize(T t, int level);
}
