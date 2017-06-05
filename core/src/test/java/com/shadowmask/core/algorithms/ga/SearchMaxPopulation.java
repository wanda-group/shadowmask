package com.shadowmask.core.algorithms.ga;

import com.google.gson.Gson;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.javatuples.Pair;
import org.shadowmask.core.algorithms.ga.Population;
import org.shadowmask.core.util.JsonUtil;

public class SearchMaxPopulation extends Population<
    SearchMaxIndividual,
    SearchMaxCrossFunction,
    MutationFunction,
    SearchMaxFitness,
    SearchMaxSelectFunction,
    SearchMaxCalculator
    > {


  SearchMaxCrossFunction searchMaxCrossFunction = new SearchMaxCrossFunction();

  MutationFunction mutationFunction = new MutationFunction();

  SearchMaxSelectFunction searchMaxSelectFunction = new SearchMaxSelectFunction();


  SearchMaxCalculator searchMaxCalculator = new SearchMaxCalculator();

  @Override protected long populationSize() {
    return 50;
  }

  @Override protected double mutationRate() {
    return 0.01;
  }

  @Override protected double crossRate() {
    return 0.7;
  }

  @Override protected long maxSteps() {
    return 1000;
  }


  @Override protected SearchMaxCrossFunction crossFunction() {
    return searchMaxCrossFunction;
  }

  @Override protected MutationFunction mutationFunction() {
    return mutationFunction;
  }

  @Override protected SearchMaxSelectFunction selectFunction() {
    return searchMaxSelectFunction;
  }

  @Override protected SearchMaxIndividual cloneIndividual(
      SearchMaxIndividual searchMaxIndividual) {
    Gson gson = JsonUtil.newGsonInstance();
    SearchMaxIndividual  individual = gson.fromJson(gson.toJson(searchMaxIndividual),SearchMaxIndividual.class);
    return individual;
  }

  @Override protected SearchMaxFitness cloneFitness(SearchMaxFitness fitness) {
    Gson gson = JsonUtil.newGsonInstance();
    return gson.fromJson(gson.toJson(fitness),SearchMaxFitness.class);
  }

  @Override protected void foundNewSolution(
      SearchMaxIndividual searchMaxIndividual, SearchMaxFitness fitness) {
    System.out.println(searchMaxIndividual.chromosome.xValue+"\t"+fitness.value);
  }

  @Override protected Map<SearchMaxIndividual, SearchMaxFitness> calculateFitness(
      List<SearchMaxIndividual> individuals) {
    Map<SearchMaxIndividual, SearchMaxFitness> fitnessMap = new HashMap<>();
    for (SearchMaxIndividual individual : individuals) {
      fitnessMap.put(individual, searchMaxCalculator.calculate(individual));
    }
    return fitnessMap;
  }

  @Override protected void init() {
    this.individuals = new ArrayList<>();
    for (int i = 0; i < this.populationSize(); ++i) {
      this.individuals.add(new SearchMaxIndividual(new SearchMaxChromosome()));
    }
    this.nextIndividuals = new ArrayList<>();
  }

  @Override protected void finished() {

  }
}
