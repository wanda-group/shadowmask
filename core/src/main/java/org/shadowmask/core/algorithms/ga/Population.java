package org.shadowmask.core.algorithms.ga;

import org.shadowmask.core.algorithms.ga.functions.CrossFunction;
import org.shadowmask.core.algorithms.ga.functions.MutationFunction;
import org.shadowmask.core.algorithms.ga.functions.SelectFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.javatuples.Pair;

public abstract class Population<
    IND extends Individual,
    CROSS extends CrossFunction<IND>,
    MUTATE extends MutationFunction<IND>,
    FIT extends Fitness,
    SELECT extends SelectFunction<IND, FIT>,
    FC extends FitnessCalculator<FIT, IND>
    > {


  abstract long populationSize();

  abstract double mutationRate();

  abstract double crossRate();

  abstract long maxSteps();

  abstract List<IND> individuals();

  abstract CROSS crossFunction();

  abstract MUTATE mutationFunction();

  abstract SELECT selectFunction();

  abstract List<FIT> evaluateFitness(Map<IND, FIT> all);

  abstract FC fitnessCalculator();

  abstract void found(List<FIT> fits);

  abstract void collect(IND individual);

  /**
   * maybe most calculation expensive method
   */
  abstract Map<IND, FIT> calculateFitness(List<IND> individuals);

  abstract void init();

  abstract void searchedOnce(long i);

  abstract void finished();

  public void evolve() {
    init();
    for (long i = 0; i < maxSteps(); ++i) {
      Map<IND, FIT> fits = calculateFitness(individuals());
      List<FIT> fitList = evaluateFitness(fits);
      if (fitList != null && fitList.size() > 0) {
        found(fitList);
        break;
      }
      // select
      List<IND> selectedInds = selectFunction().select(individuals(), fits, populationSize());
      // cross
      List<Pair<IND, IND>> pairs = generateCrossPairs(selectedInds);
      for (Pair<IND, IND> pair : pairs) {
        Pair<IND, IND> newPair = null;
        Double crossR = Math.random();
        if (crossR < crossRate()) {
          newPair = crossFunction().cross(pair.getValue0(), pair.getValue1());
        }
        if (newPair == null) {
          newPair = pair;
        }
        Double mutateR = Math.random();
        if (mutateR < mutationRate()) {
          newPair = new Pair<IND, IND>(mutationFunction().mutate(newPair.getValue0()),
              newPair.getValue1());
        }
        mutateR = Math.random();
        if (mutateR < mutationRate()) {
          newPair = new Pair<IND, IND>(newPair.getValue0(),
              mutationFunction().mutate((newPair.getValue1())));
        }
        collect(newPair.getValue0());
        collect(newPair.getValue1());
      }
      searchedOnce(i);
    }
    finished();
  }


  /**
   * maybe change the population order , no affection however
   */
  protected List<Pair<IND, IND>> generateCrossPairs(List<IND> inds) {
    List<Pair<IND, IND>> pairs = new ArrayList<>();
    for (int i = 0; i < inds.size(); i += 2) {
      int idx = new Random().nextInt(inds.size() - i - 1) + i + 1;
      pairs.add(new Pair<IND, IND>(inds.get(i), inds.get(idx)));
      Collections.swap(inds, i, idx);
    }
    return pairs;
  }


}
