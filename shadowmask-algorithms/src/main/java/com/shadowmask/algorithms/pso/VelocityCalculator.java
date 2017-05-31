package com.shadowmask.algorithms.pso;

public interface VelocityCalculator<V extends Velocity, P extends Position, F extends Fitness> {

  V newVelocity(V currentV,P currentPosition, F currentFitness, P historyBestPostion,
      F historyBestFitness, P globalBestPosition, F globalBestFitness,
      P currentBestPosition, F currentBestFitness, P currentWorstPosition,
      F currentWorstFitness);

}
