package com.shadowmask.core.algorithms.pso;

import org.shadowmask.core.algorithms.pso.FitnessCalculator;

public class PSOTFitnessCalculator
    implements FitnessCalculator<PSOTParticle, PSOTFitness> {

  @Override public PSOTFitness fitness(PSOTParticle psotParticle) {
    return PSOTFitness
        .valueOf(PSOTTarget.func(psotParticle.currentPosition.xValue));
  }
}
