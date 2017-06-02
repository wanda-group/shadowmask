package com.shadowmask.core.algorithms.ga;

import java.math.BigDecimal;
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
    Double total = null;

    for (SearchMaxFitness fit : fits.values()) {
      if (min == null) {
        min = fit.value;
      } else {
        if (fit.value.compareTo(min) < 0) {
          min = fit.value;
        }
      }
    }

    for (SearchMaxFitness fit : fits.values()) {
      if (total == null) {
        total = fit.value-min;
      } else {
        total = total + (fit.value-min);
      }
    }

    if (total.longValue() == 0) {
      return ind;
    }
    List<SearchMaxIndividual> selectTab = new ArrayList<>();
    for (SearchMaxIndividual individual : ind) {
      Double prop = (fits.get(individual).value - min)/total;
      Long num = Double.valueOf(selectNum*prop+1).longValue();
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
