package com.shadowmask.core.algorithms.ga;

import java.math.BigDecimal;
import java.util.Random;
import org.javatuples.Pair;
import org.shadowmask.core.algorithms.ga.Chromosome;

public class SearchMaxChromosome implements Chromosome<SearchMaxChromosome> {

  public BigDecimal xValue;

  public SearchMaxGeneCoder coder  = new SearchMaxGeneCoder();

  public SearchMaxGene gene;

  public SearchMaxChromosome(BigDecimal xValue) {
    this.xValue = xValue;
  }

  public SearchMaxChromosome() {
    this.xValue = BigDecimal.valueOf(Math.random()*(SearchMaxBounds.hBound-SearchMaxBounds.lBound));
  }

  @Override
  public Pair<SearchMaxChromosome, SearchMaxChromosome> cross(SearchMaxChromosome that) {
    Pair<SearchMaxGene,SearchMaxGene> newGene = this.getGene().cross(that.getGene());
    return new Pair<>(coder.decode(newGene.getValue0()),coder.decode(newGene.getValue1()));
  }

  @Override
  public SearchMaxChromosome mutate() {
    return coder.decode(this.getGene().mutate());
  }

  public SearchMaxGene getGene(){
    if(gene == null){
      gene = coder.encode(this);
    }
    return gene;
  }

}
