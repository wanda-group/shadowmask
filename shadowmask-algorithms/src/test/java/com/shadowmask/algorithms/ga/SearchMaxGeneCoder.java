package com.shadowmask.algorithms.ga;

public class SearchMaxGeneCoder implements GeneCoder<SearchMaxChromosome, SearchMaxGene> {

  @Override
  public SearchMaxGene encode(SearchMaxChromosome chromosome) {
    return new SearchMaxGene(Integer.toBinaryString(chromosome.xValue));
  }

  @Override
  public SearchMaxChromosome decode(SearchMaxGene gene) {
    return new SearchMaxChromosome(Integer.parseUnsignedInt(gene.binStr,2));
  }
}