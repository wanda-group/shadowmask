package com.shadowmask.algorithms.ga;

import java.util.Random;
import org.javatuples.Pair;

public class SearchMaxChromosome implements Chromosome<SearchMaxChromosome> {

  public int xValue;

  public SearchMaxGeneCoder coder  = new SearchMaxGeneCoder();

  public SearchMaxGene gene;

  public SearchMaxChromosome(int xValue) {
    this.xValue = xValue;
  }

  public SearchMaxChromosome() {
    this.xValue = new Random().nextInt(100);
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
