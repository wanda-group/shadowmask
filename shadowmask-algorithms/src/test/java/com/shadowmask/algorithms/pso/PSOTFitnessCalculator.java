package com.shadowmask.algorithms.pso;

public class PSOTFitnessCalculator
    implements FitnessCalculator<PSOTParticle, PSOTFitness> {

  @Override public PSOTFitness fitness(PSOTParticle psotParticle) {
    return PSOTFitness
        .valueOf(PSOTTarget.func(psotParticle.currentPosition.xValue));
  }
}
