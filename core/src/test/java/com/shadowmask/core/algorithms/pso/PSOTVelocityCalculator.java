package com.shadowmask.core.algorithms.pso;

import org.shadowmask.core.algorithms.pso.VelocityCalculator;

public abstract class PSOTVelocityCalculator
    implements VelocityCalculator<PSOTVelocity, PSOTPosition, PSOTFitness> {

  public abstract double randomSearchRate();

  public abstract double lBound();
  public abstract double hBound();

  @Override public PSOTVelocity newVelocity(PSOTVelocity currentV,PSOTPosition currentPosition,
      PSOTFitness currentFitness, PSOTPosition historyBestPostion,
      PSOTFitness historyBestFitness, PSOTPosition globalBestPosition,
      PSOTFitness globalBestFitness, PSOTPosition currentBestPosition,
      PSOTFitness currentBestFitness, PSOTPosition currentWorstPosition,
      PSOTFitness currentWorstFitness) {
    double rate = Math.random();
    if (rate < randomSearchRate()) {
      double value = Math.random() * (hBound() - lBound());
      PSOTVelocity v = new PSOTVelocity();
      v.velocity = value;
      return v;
    }
    double divider =
        currentBestFitness.value - currentWorstFitness.value;
    double stayRate = 0;
    if (divider * 1000000 != 0) {
      stayRate = (currentFitness.value - currentWorstFitness.value)
          / divider;
    }

    divider =
        globalBestFitness.value - currentFitness.value;
    double selfLearnRate = 1;

    if (divider * 1000000 != 0) {
      selfLearnRate =
          (historyBestFitness.value - currentFitness.value) / divider;
    }
    double learnOther = 1 - selfLearnRate;
    double v =
        currentV.velocity * stayRate + selfLearnRate * Math.random() * (
            historyBestPostion.xValue - currentPosition.xValue)
            + learnOther * Math.random() * (
            globalBestPosition.xValue
                - currentPosition.xValue);
    PSOTVelocity vv = new PSOTVelocity();
    vv.velocity = v;
    return vv;
  }
}
