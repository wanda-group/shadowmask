package org.shadowmask.core.algorithms.pso;

public interface Particle<P extends Position, V extends Velocity, F extends Fitness> {

  /**
   * init operation .
   */
  void init();

  /**
   * move to a new position
   */
  void move(V v);

  /**
   * current velocity
   */
  V currentVelocity();

  /**
   * current position
   */
  P currentPosition();

  /**
   * current fitness
   */
  F currentFitness();

  /**
   * best position since beginning.
   */
  P historyBestPosition();

  /**
   * best fitness since beginning.
   */
  F historyBestFitness();

  /**
   *
   * @param velocity
   */
  void updateVelocity(V velocity);

  /**
   * get better
   */
  void getBetter(P betterPosition, F betterFitness);

}
