package com.shadowmask.algorithms.ga;

import com.shadowmask.algorithms.ga.functions.SelectFunction;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SearchMaxSelectFunction implements
    SelectFunction<SearchMaxIndividual, SearchMaxFitness> {

  @Override
  public List<SearchMaxIndividual> select(List<SearchMaxIndividual> ind,
      Map<SearchMaxIndividual, SearchMaxFitness> fits, long selectNum) {


    BigDecimal min = fits.values().stream().map(fit -> fit.value)
        .reduce((d1, d2) -> d1.compareTo(d2)>0?d2:d1).get();
    BigDecimal total = fits.values().stream().map(fit -> fit.value)
        .reduce((d1, d2) -> d1.subtract(min).add(d2.subtract(min))).get();
    if(total.longValue() == 0){
      return ind;
    }
    List<SearchMaxIndividual> selectTab = new ArrayList<>();
    for (SearchMaxIndividual individual : ind) {
      BigDecimal prop = fits.get(individual).value.subtract(min).divide(total,4,BigDecimal.ROUND_HALF_UP);
      Long num = BigDecimal.valueOf(selectNum) .multiply(prop).longValue() ;
      if (num == 0L) {
        num = 1L;
      }else {
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
