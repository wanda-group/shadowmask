package com.shadowmask.algorithms.ga;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;

public class SearchMaxPopulation extends Population<
    SearchMaxIndividual,
    SearchMaxCrossFunction,
    MutationFunction,
    SearchMaxFitness,
    SearchMaxSelectFunction,
    SearchMaxCalculator
    > {

  List<SearchMaxIndividual> individuals = null;

  List<SearchMaxIndividual> newIndividuals = null;

  SearchMaxCrossFunction searchMaxCrossFunction = new SearchMaxCrossFunction();

  MutationFunction mutationFunction = new MutationFunction();

  SearchMaxSelectFunction searchMaxSelectFunction = new SearchMaxSelectFunction();


  SearchMaxCalculator searchMaxCalculator = new SearchMaxCalculator();

  @Override
  long populationSize() {
    return 50;
  }

  @Override
  double mutationRate() {
    return 0.01;
  }

  @Override
  double crossRate() {
    return 0.4;
  }

  @Override
  long maxSteps() {
    return 100000;
  }

  @Override
  List<SearchMaxIndividual> individuals() {
    return individuals;
  }

  @Override
  SearchMaxCrossFunction crossFunction() {
    return searchMaxCrossFunction;
  }

  @Override
  MutationFunction mutationFunction() {
    return mutationFunction;
  }

  @Override
  SearchMaxSelectFunction selectFunction() {
    return searchMaxSelectFunction;
  }

  @Override
  List<SearchMaxFitness> evaluateFitness(Map<SearchMaxIndividual, SearchMaxFitness> all) {
    final BigDecimal[] d = {null};
    final String[] g = {null};
    all.keySet().stream().map(ind -> new Pair<>(ind.chromosome.getGene(), all.get(ind).value))
        .forEach(kv -> {
          if (d[0] == null) {
            d[0] = kv.getValue1();
            g[0] = kv.getValue0().binStr;
          } else if (d[0].compareTo(kv.getValue1()) < 0) {
            d[0] = kv.getValue1();
            g[0] = kv.getValue0().binStr;
          }
        });
    System.out.print(g[0] + "\t" + Integer.parseInt(g[0], 2) + "\t" + d[0]);
    all.keySet().stream().forEach(k -> {
      System.out.print("\t" + k.chromosome.xValue);
    });
    System.out.println();

    return null;
  }


  @Override
  SearchMaxCalculator fitnessCalculator() {
    return searchMaxCalculator;
  }

  @Override
  void found(List<SearchMaxFitness> searchMaxFitnesses) {
    System.out.println(searchMaxFitnesses);
  }

  @Override
  void collect(SearchMaxIndividual individual) {
    newIndividuals.add(individual);
  }

  @Override
  Map<SearchMaxIndividual, SearchMaxFitness> calculateFitness(
      List<SearchMaxIndividual> individuals) {
    Map<SearchMaxIndividual, SearchMaxFitness> fitnessMap = new HashMap<>();
    for (SearchMaxIndividual individual : individuals) {
      fitnessMap.put(individual, searchMaxCalculator.calculate(individual));
    }
    return fitnessMap;
  }

  @Override
  void init() {
    this.individuals = new ArrayList<>();
    for (int i = 0; i < this.populationSize(); ++i) {
      this.individuals.add(new SearchMaxIndividual(new SearchMaxChromosome()));
    }
    this.newIndividuals = new ArrayList<>();
  }

  @Override
  void searchedOnce(long i) {
    individuals = newIndividuals;
    this.newIndividuals = new ArrayList<>();
  }

  @Override
  void finished() {

  }
}
