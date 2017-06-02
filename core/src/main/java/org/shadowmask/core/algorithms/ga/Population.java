package org.shadowmask.core.algorithms.ga;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.javatuples.Pair;
import org.shadowmask.core.algorithms.ga.functions.CrossFunction;
import org.shadowmask.core.algorithms.ga.functions.MutationFunction;
import org.shadowmask.core.algorithms.ga.functions.SelectFunction;

public abstract class Population<IND extends Individual, CROSS extends CrossFunction<IND>, MUTATE extends MutationFunction<IND>, FIT extends Fitness, SELECT extends SelectFunction<IND, FIT>, FC extends FitnessCalculator<FIT, IND>> {

  protected List<IND> individuals;

  protected List<IND> nextIndividuals;

  // best individual till now
  private IND bestIndividual;
  // best fitness till now
  private FIT bestFitness;

  protected abstract long populationSize();

  protected abstract double mutationRate();

  protected abstract double crossRate();

  protected abstract long maxSteps();

  public List<IND> individuals() {
    return individuals;
  }

  protected abstract CROSS crossFunction();

  protected abstract MUTATE mutationFunction();

  protected abstract SELECT selectFunction();

  protected abstract IND cloneIndividual(IND ind);

  protected abstract FIT cloneFitness(FIT fitness);

  protected abstract void foundNewSolution(IND ind, FIT fitness);

  /**
   * maybe most calculation expensive method
   */
  protected abstract Map<IND, FIT> calculateFitness(List<IND> individuals);

  protected void init() {
    this.individuals = new ArrayList<>();
    nextIndividuals = new ArrayList<>();
  }


  protected abstract void finished();

  public void evolve() {
    this.init();
    for (long i = 0; i < maxSteps(); ++i) {
      Map<IND, FIT> fits = calculateFitness(individuals());
      for (IND ind : fits.keySet()) {
        if (this.bestIndividual == null) {
          this.bestIndividual = ind;
          this.bestFitness = fits.get(ind);
        } else {
          if (fits.get(ind).betterThan(this.bestFitness)) {
            this.bestFitness = cloneFitness(fits.get(ind));
            this.bestIndividual = cloneIndividual(ind);
            foundNewSolution(this.bestIndividual, this.bestFitness);
          }
        }
      }
      // select
      List<IND> selectedInds =
          selectFunction().select(individuals(), fits, populationSize());
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
          newPair =
              new Pair<IND, IND>(mutationFunction().mutate(newPair.getValue0()),
                  newPair.getValue1());
        }
        mutateR = Math.random();
        if (mutateR < mutationRate()) {
          newPair = new Pair<IND, IND>(newPair.getValue0(),
              mutationFunction().mutate((newPair.getValue1())));
        }
        nextIndividuals.add(newPair.getValue0());
        nextIndividuals.add(newPair.getValue1());
      }
      individuals = nextIndividuals;
      nextIndividuals = new ArrayList<>();
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
