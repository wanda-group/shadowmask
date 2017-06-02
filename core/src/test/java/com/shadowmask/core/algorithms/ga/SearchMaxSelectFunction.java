package com.shadowmask.core.algorithms.ga;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.shadowmask.core.algorithms.ga.functions.SelectFunction;

public class SearchMaxSelectFunction
    implements SelectFunction<SearchMaxIndividual, SearchMaxFitness> {

  @Override public List<SearchMaxIndividual> select(
      List<SearchMaxIndividual> ind,
      Map<SearchMaxIndividual, SearchMaxFitness> fits, long selectNum) {

    Double min = null;
    Double max = null;
    Double total = null;

    for (SearchMaxFitness fit : fits.values()) {
      if (min == null) {
        min = fit.value;
      } else {
        if (fit.value.compareTo(min) < 0) {
          min = fit.value;
        }
      }
      if (max == null) {
        max = fit.value;
      } else {
        if (fit.value > max) {
          max = fit.value;
        }
      }

    }

    for (SearchMaxFitness fit : fits.values()) {
      if (total == null) {
        total = max - fit.value;
      } else {
        total = total + (max - fit.value);
      }
    }

    if (total.longValue() == 0) {
      return ind;
    }
    List<SearchMaxIndividual> selectTab = new ArrayList<>();
    for (SearchMaxIndividual individual : ind) {
      Double prop = (max - fits.get(individual).value) / total;
      Long num = Double.valueOf(selectNum * prop + 1).longValue();
      if (num == 0L) {
        num = 0L;
      } else {
        //        System.out.println(prop);
      }
      for (long i = 0; i < num; ++i) {
        selectTab.add(individual);
      }
    }

    List<SearchMaxIndividual> result = new ArrayList<>();
    int index = 0;
    for (int i = 0; i < selectNum; ++i) {
      index = new Random().nextInt(selectTab.size());
      SearchMaxIndividual individual = selectTab.get(index);
      result.add(individual);
    }
    return result;
  }

}
