package com.shadowmask.core.algorithms.ga;

import java.math.BigDecimal;
import org.shadowmask.core.algorithms.ga.GeneCoder;

public class SearchMaxGeneCoder implements
    GeneCoder<SearchMaxChromosome, SearchMaxGene> {

  @Override
  public SearchMaxGene encode(SearchMaxChromosome chromosome) {
    return new SearchMaxGene(chromosome.xValue.doubleValue());
  }

  @Override
  public SearchMaxChromosome decode(SearchMaxGene gene) {
    return new SearchMaxChromosome(BigDecimal.valueOf(gene.xValue));
  }
}