package com.shadowmask.algorithms.ga;

import com.shadowmask.algorithms.ga.Chromosome;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.javatuples.Pair;

public abstract class Individual<CHROMOSOMES extends Chromosome<CHROMOSOMES>, IND extends Individual<CHROMOSOMES, IND>> {

  public abstract List<CHROMOSOMES> chromosomes();

  public abstract IND constructIndividualFromChromosome(List<CHROMOSOMES> l1);

  public Pair<IND, IND> cross(IND another) {
    List<CHROMOSOMES> l1 = new ArrayList<CHROMOSOMES>();
    List<CHROMOSOMES> l2 = new ArrayList<CHROMOSOMES>();
    for (int i = 0; i < chromosomes().size(); ++i) {
      Pair<CHROMOSOMES, CHROMOSOMES> chPair = chromosomes().get(i)
          .cross(another.chromosomes().get(i));
      l1.add(chPair.getValue0());
      l2.add(chPair.getValue1());
    }
    return new Pair<IND, IND>(constructIndividualFromChromosome(l1),
        constructIndividualFromChromosome(l2));
  }


  public IND mutate() {
    return constructIndividualFromChromosome(
        chromosomes().stream().map(ch -> ch.mutate()).collect(Collectors.toList())
    );
  }

}
