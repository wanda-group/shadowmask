package com.shadowmask.algorithms.pso;

import java.util.List;
import java.util.Map;

public abstract class Swarm<V extends Velocity, F extends Fitness, P extends Position, PA extends Particle<P, V, F>> {

  private PA globalBest;

  private PA currentBest;

  private PA currentWorst;

  Map<PA, F> fitnessMap = null;

  Map<PA, V> newVelocities = null;

  /**
   * all particles
   */
  public abstract List<PA> particles();

  /**
   * calculate current fitness for all particles
   */
  public abstract Map<PA, F> calculateFitness();


  /**
   * calculate current fitness for all particles
   */
  public Map<PA, F> currentFitness(){
    return fitnessMap;
  }

  /**
   * calculate current velocity for all particles
   */
  public abstract Map<PA, V> calculateNewVelocities();

  public Map<PA, V> velocityMap(){
    return newVelocities;
  }

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
      Map<PA, F> fitnessMap = calculateFitness();
      updateCurrentBestParticle(null);
      updateCurrentWorstParticle(null);
      // update swarm information
      particles.forEach(pa -> {
        F f = fitnessMap.get(pa);

        if (pa.historyBestPosition() == null || pa.historyBestFitness() == null
            || f.betterThan(pa.historyBestFitness())) {
          pa.getBetter(pa.currentPosition(), f);
        }
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

      Map<PA, V> velocities = calculateNewVelocities();

      // move to new position
      particles.forEach(pa -> {
        pa.move(velocities.get(pa));
      });
    }
  }

}
