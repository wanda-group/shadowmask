package com.shadowmask.core.algorithms.ga;

import java.util.Random;
import org.javatuples.Pair;
import org.shadowmask.core.algorithms.ga.Gene;

public class SearchMaxGene implements Gene<SearchMaxGene> {

  public Double xValue;

  private Double scale = 10D;

  public SearchMaxGene(Double xValue) {
    this.xValue = xValue;
  }

  @Override public Pair<SearchMaxGene, SearchMaxGene> cross(
      SearchMaxGene that) {
    Double step = Math.abs(this.xValue - that.xValue) * Math.random();
    Pair<Double, Double> minMax = this.xValue < that.xValue
        ? new Pair<>(this.xValue, that.xValue)
        : new Pair<>(that.xValue, this.xValue);
    Double g1 = minMax.getValue0() + step;
    Double g2 = minMax.getValue1() - step;
    return new Pair<>(new SearchMaxGene(g1),new SearchMaxGene(g2));
  }

  @Override public SearchMaxGene mutate() {
    return new SearchMaxGene(Math.random()*(SearchMaxBounds.hBound-SearchMaxBounds.lBound)+SearchMaxBounds.lBound);
  }
}
