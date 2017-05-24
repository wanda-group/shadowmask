package com.shadowmask.algorithms.ga.functions;

import com.shadowmask.algorithms.ga.Fitness;
import com.shadowmask.algorithms.ga.Individual;
import java.util.List;
import java.util.Map;

public interface SelectFunction<IND extends Individual, FIT extends Fitness> {

  List<IND> select(List<IND> ind, Map<IND, FIT> fits, long selectNum);
}
