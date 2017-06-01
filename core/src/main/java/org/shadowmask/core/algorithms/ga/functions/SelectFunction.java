package org.shadowmask.core.algorithms.ga.functions;

import org.shadowmask.core.algorithms.ga.Fitness;
import org.shadowmask.core.algorithms.ga.Individual;
import java.util.List;
import java.util.Map;

public interface SelectFunction<IND extends Individual, FIT extends Fitness> {

  List<IND> select(List<IND> ind, Map<IND, FIT> fits, long selectNum);
}
