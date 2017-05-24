package com.shadowmask.algorithms.ga;

import java.util.Collections;
import java.util.List;

public class SearchMaxIndividual extends Individual<SearchMaxChromosome, SearchMaxIndividual> {


  public SearchMaxChromosome chromosome;

  public List<SearchMaxChromosome> chromosomes;

  public SearchMaxIndividual(SearchMaxChromosome chromosome) {
    if (chromosome == null) {
      throw new NullPointerException();
    }
    this.chromosome = chromosome;
    this.chromosomes = Collections.singletonList(this.chromosome);
  }

  @Override
  public List<SearchMaxChromosome> chromosomes() {
    return chromosomes;
  }

  @Override
  public SearchMaxIndividual constructIndividualFromChromosome(List<SearchMaxChromosome> l1) {
    return new SearchMaxIndividual(l1.get(0));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SearchMaxIndividual that = (SearchMaxIndividual) o;

    return chromosome != null ? chromosome.getGene().binStr.equals(that.chromosome.getGene().binStr) : that.chromosome == null;
  }

  @Override
  public int hashCode() {
    return chromosome != null ? chromosome.getGene().binStr.hashCode() : 0;
  }
}
