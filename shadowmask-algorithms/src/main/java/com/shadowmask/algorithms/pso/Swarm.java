package com.shadowmask.algorithms.pso;

import java.util.List;

public abstract class Swarm<V extends Velocity, F extends Fitness, P extends Position, S extends Swarm, PA extends Particle<P, V, F, S>> {

  private PA globalBest;

  private PA currentBest;

  private PA currentWorst;

  /**
   * all particles
   */
  public abstract List<PA> particles();

  /**
   * best particle till now .
   */
  public PA globalBestParticle() {
    return globalBest;
  }

  /**
   * found a new global best particle ,
   */
  public void updateGlobalBestParticle(PA pa) {
    this.globalBest = pa;
  }

  /**
   * best particle in current swarm
   */
  public PA currentBestParticle() {
    return currentBest;
  }

  /**
   * found current best particle
   */
  public void updateCurrentBestParticle(PA p) {
    this.currentBest = p;
  }

  /**
   * worst particle in current swarm
   */
  public PA currentWorstParticle() {
    return currentWorst;
  }

  /**
   * update a current worst particle .
   */
  public void updateCurrentWorstParticle(PA p) {
    this.currentWorst = p;
  }

  /**
   * max steps
   */
  public abstract int maxSteps();

  public abstract int particleSize();

  public void optimize() {
    for (int i = 0; i < maxSteps(); ++i) {
      List<PA> particles = particles();
      updateCurrentBestParticle(null);
      updateCurrentWorstParticle(null);
      // update swarm information
      particles.forEach(pa -> {
        F f = pa.currentFitness();
        // update global best
        if (globalBestParticle() == null || f
            .betterThan(globalBestParticle().historyBestFitness())) {
          updateGlobalBestParticle(pa);
        }
        // update current best
        if (currentBestParticle() == null || f
            .betterThan(currentBestParticle().currentFitness())) {
          updateCurrentBestParticle(pa);
        }
        // update current worst
        if (currentWorstParticle() == null || currentWorstParticle()
            .currentFitness().betterThan(f)) {
          updateCurrentWorstParticle(pa);
        }
      });

      // move to new position
      particles.forEach(pa -> {
        pa.move();
      });
    }
  }

}
